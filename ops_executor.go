package flashback

import (
	"errors"
	"fmt"
	"gopkg.in/mgo.v2"
	"strings"
	"time"
)

var (
	NotSupported = errors.New("op type not supported")
)

type execute func(content Document, textContent string, collection *mgo.Collection) error

type OpsExecutor struct {
	session   *mgo.Session
	statsChan chan OpStat
	logger    *Logger

	// keep track of the results retrieved by find(). For verification purpose
	// only.
	lastResult  interface{}
	lastLatency time.Duration
	subExecutes map[OpType]execute
}

func NewOpsExecutor(session *mgo.Session, statsChan chan OpStat, logger *Logger) *OpsExecutor {
	e := &OpsExecutor{
		session:   session,
		statsChan: statsChan,
		logger:    logger,
	}

	e.subExecutes = map[OpType]execute{
		Query:         e.execQuery,
		Insert:        e.execInsert,
		Update:        e.execUpdate,
		Remove:        e.execRemove,
		Count:         e.execCount,
		FindAndModify: e.execFindAndModify,
	}
	return e
}

// Given a JSON of the op (as a raw string) and a key (e.g. $hint or $orderby),
// extract the arguments, transforming { organization: 1, date_created: -1 }
// into a list ["organization", "-date_created"].
func getArgs(textContent string, key string) []string {

	// Initialize a list of arguments
	args := make([]string, 0)

	// Find the key we're looking for in the string
	start := strings.Index(textContent, "\""+key+"\"")
	textContent = textContent[start+len(key)+2:]

	// Parse the string assuming proper format (it's safe to do so here
	// because the string we use has already been parsed to JSON).
	// States of the parser:
	// 0 - looking for key
	// 1 - in the key (looking for its end)
	// 2 - looking for direction of the index (after found, resets state to 0)
	state := 0
	buffer := ""
	for _, c := range textContent {
		if state == 0 && c == '}' {
			break
		} else if state == 0 && c == '"' {
			state = 1
		} else if state == 1 {
			if c == '"' {
				args = append(args, buffer)
				buffer = ""
				state = 2
			} else {
				buffer += string(c)
			}
		} else if state == 2 && c == '-' {
			args[len(args)-1] = "-" + args[len(args)-1]
		} else if state == 2 && c == '1' {
			state = 0
		}
	}
	return args
}

func (e *OpsExecutor) execQuery(content Document, textContent string, coll *mgo.Collection) error {
	var query *mgo.Query
	result := []Document{}

	// cast the query to a map of string keys to interfaces
	q, ok := content["query"].(map[string]interface{})

	// If the query contains $hint or $orderby, we need to make sure that these
	// are applied via Query.Hint and Query.Sort with the correct order
	if ok && q["$query"] != nil {
		query = coll.Find(q["$query"])
		if q["$hint"] != nil {
			query.Hint(getArgs(textContent, "$hint")...)
		}
		if q["$orderby"] != nil {
			query.Sort(getArgs(textContent, "$orderby")...)
		}
	} else {
		query = coll.Find(content["query"])
	}
	if content["ntoreturn"] != nil {
		if ntoreturn, err := safeGetInt(content["ntoreturn"]); err != nil {
			e.logger.Error("could not set ntoreturn: ", err)
		} else {
			query.Limit(ntoreturn)
		}
	}
	if content["ntoskip"] != nil {
		if ntoskip, err := safeGetInt(content["ntoskip"]); err != nil {
			e.logger.Error("could not set ntoskip: ", err)
		} else {
			query.Skip(ntoskip)
		}
	}
	err := query.All(&result)
	e.lastResult = &result
	return err
}

func (e *OpsExecutor) execInsert(content Document, textContent string, coll *mgo.Collection) error {
	return coll.Insert(content["o"])
}

func (e *OpsExecutor) execUpdate(content Document, textContent string, coll *mgo.Collection) error {
	return coll.Update(content["query"], content["updateobj"])
}

func (e *OpsExecutor) execRemove(content Document, textContent string, coll *mgo.Collection) error {
	return coll.Remove(content["query"])
}

func (e *OpsExecutor) execCount(content Document, textContent string, coll *mgo.Collection) error {
	_, err := coll.Count()
	return err
}

func (e *OpsExecutor) execFindAndModify(content Document, textContent string, coll *mgo.Collection) error {
	result := Document{}
	change := mgo.Change{Update: content["update"].(map[string]interface{})}
	_, err := coll.Find(content["query"]).Apply(change, result)
	return err
}

// We only support handful op types. This function helps us to process supported
// ops in a universal way.
//
// We do not canonicalize the ops in OpsReader because we hope ops reader to do
// its job honestly and the consumer of these ops decide how to further process
// the original ops.
func CanonicalizeOp(op *Op) *Op {
	if op.Type != Command {
		return op
	}

	cmd := op.Content["command"].(map[string]interface{})

	for _, name := range []string{"findandmodify", "count"} {
		collName, exist := cmd[name]
		if !exist {
			continue
		}

		op.Type = OpType("command." + name)
		op.Collection = collName.(string)
		op.Content = cmd

		return op
	}

	return nil
}

func retryOnSocketFailure(block func() error, session *mgo.Session, logger *Logger) error {
	err := block()
	if err == nil {
		return nil
	}

	switch err.(type) {
	case *mgo.QueryError, *mgo.LastError:
		return err
	}

	switch err {
	case mgo.ErrNotFound, NotSupported:
		return err
	}

	// Otherwise it's probably a socket error so we refresh the connection,
	// and try again
	session.Refresh()
	logger.Error("retrying mongo query after error: ", err)
	return block()
}

func (e *OpsExecutor) Execute(op *Op) error {
	startOp := time.Now()

	block := func() error {
		content := op.Content
		textContent := op.TextContent
		coll := e.session.DB(op.Database).C(op.Collection)
		return e.subExecutes[op.Type](content, textContent, coll)
	}
	err := retryOnSocketFailure(block, e.session, e.logger)

	latencyOp := time.Now().Sub(startOp)
	e.lastLatency = latencyOp

	if e.statsChan != nil {
		if err == nil {
			e.statsChan <- OpStat{op.Type, latencyOp, false}
		} else {
			// error condition
			e.statsChan <- OpStat{op.Type, latencyOp, true}
		}
	}

	return err
}

func (e *OpsExecutor) LastLatency() time.Duration {
	return e.lastLatency
}

func safeGetInt(i interface{}) (int, error) {
	switch i.(type) {
	case int32:
		return int(i.(int32)), nil
	case int64:
		return int(i.(int64)), nil
	case float32:
		return int(i.(float32)), nil
	case float64:
		return int(i.(float64)), nil
	default:
		return int(0), fmt.Errorf("unsupported type for %i", i)
	}
}
