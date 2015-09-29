package flashback

import (
	"time"
)

// OpType is the name of mongo op type
type OpType string

// Document represents the json-like infromation of an op
type Document map[string]interface{}

// Contains a list of mongo op types
const (
	Insert        OpType = "insert"
	Update        OpType = "update"
	Remove        OpType = "remove"
	Query         OpType = "query"
	Command       OpType = "command"
	Count         OpType = "command.count"
	FindAndModify OpType = "command.findandmodify"
)

// AllOpTypes specifies all supported op types
var AllOpTypes = []OpType{
	Insert,
	Update,
	Remove,
	Query,
	Count,
	FindAndModify,
}

// Op represents a MongoDB operation that contains enough details to be
// replayed.
type Op struct {
	Database   string
	Collection string
	Type       OpType
	Timestamp  time.Time // indicates when this op was performed

	// The details of this op, which may vary for different op types.
	Content Document

	// TextContent contains an original JSON in a string format describing the
	// operation as seen in the file that we load the operations from. It's
	// order to determine the correct $orderby and $hint values, since the
	// Unmarshalled version of the JSON stored in the Content Document doesn't
	// Preserve the right order of the keys in the map.
	TextContent string
}
