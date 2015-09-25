package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ParsePlatform/flashback"
	"gopkg.in/mgo.v2"
)

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

var (
	maxOps                   int
	numSkipOps               int
	opsFilename              string
	slowOpThresholdMs        int
	socketTimeout            int64
	startTime                int64
	style                    string
	cyclic                   bool
	url                      string
	challengerUrl            string
	challengerUrl2           string
	challengerUrl3           string
	verbose                  bool
	workers                  int
	stderr                   string
	stdout                   string
	logger                   *flashback.Logger
	statsFilename            string
	challengerStatsFilename  string
	challengerStatsFilename2 string
	challengerStatsFilename3 string
	opFilter                 string
	speedup                  float64
)

const (
	// Set one minute timeout on mongo socket connections (nanoseconds) by default
	defaultMgoSocketTimeout = 60000000000
)

func init() {
	flag.StringVar(&opsFilename,
		"ops_filename",
		"",
		"The file for the serialized ops, generated by the Record scripts.")
	flag.StringVar(&url,
		"url",
		"",
		"[Optional] The database server's url, in the format of <host>[:<port>]. Defaults to localhost:27017")
	flag.StringVar(&challengerUrl,
		"challenger_url",
		"",
		"[Optional] Url of the challenger, another mongo database configured with different parameters. "+
			"Queries will be sent into both simultaneously Format: <host>[:<port>]. Not used by default. "+
			"Supported by only \"real\" style")
	flag.StringVar(&challengerUrl2,
		"challenger_url2",
		"",
		"[Optional] Url of the challenger2, another mongo database configured with different parameters. "+
			"Queries will be sent into both simultaneously Format: <host>[:<port>]. Not used by default. "+
			"Supported by only \"real\" style")
	flag.StringVar(&challengerUrl3,
		"challenger_url3",
		"",
		"[Optional] Url of the challenger3, another mongo database configured with different parameters. "+
			"Queries will be sent into both simultaneously Format: <host>[:<port>]. Not used by default. "+
			"Supported by only \"real\" style")
	flag.StringVar(&style,
		"style",
		"",
		"How to replay the the ops. You can choose: \n"+
			"	stress: replay ops as fast as possible\n"+
			"	real: replay ops in accordance to ops' timestamps")
	flag.Float64Var(&speedup,
		"speedup",
		1.0,
		"This option is for \"real\" style. Instead of replaying ops realtime, you can use this option "+
			"to speedup or slowdown execution. For example, setting speedup to 2 will send ops 2x faster")
	flag.BoolVar(&cyclic,
		"cyclic",
		false,
		"In \"real\" style, if true, we are going to cycle through the ops infinitely. If false, we will execute all the ops only once")
	flag.IntVar(&workers,
		"workers",
		10,
		"[Optional] Number of workers that sends ops to database.")
	flag.IntVar(&maxOps,
		"maxOps",
		math.MaxUint32, // default value for maxOps is maxUint32
		"[Optional] Maximal amount of ops to be replayed from the "+
			"ops_filename file. By setting it to `0`, replayer will "+
			"replay all the ops.")
	flag.IntVar(&numSkipOps,
		"numSkipOps",
		0,
		"[Optional] Skip first N ops. Useful for when the total ops in ops_filename"+
			" exceeds available memory and you're running in stress mode.")
	flag.Int64Var(&socketTimeout,
		"socketTimeout",
		defaultMgoSocketTimeout,
		"[Optional] Mongo socket timeout in nanoseconds.")
	flag.IntVar(&slowOpThresholdMs,
		"slow_op_threshold_ms",
		0,
		"[Optional] All ops that take longer than slow_op_threshold_ms will be logged. Turned off by default.")
	flag.BoolVar(&verbose,
		"verbose",
		false,
		"[Optional] Print op errors and other verbose information to stdout.")
	flag.Int64Var(&startTime,
		"start_time",
		0,
		"[Optional] Provide a unix timestamp (i.e. 1396456709419)"+
			"indicating the first op that you want to run. Otherwise, play from the top.")
	flag.StringVar(&stderr,
		"stderr",
		"",
		"[Optional] Write error/warning log messages to specified file, instead of stderr.")
	flag.StringVar(&stdout,
		"stdout",
		"",
		"[Optional] Write regular log messages to specified file, instead of stdout.")
	flag.StringVar(&statsFilename,
		"statsfilename",
		"",
		"[Optional] Provide a path to a file that will store the stats analyzer output at each interval.")
	flag.StringVar(&challengerStatsFilename,
		"challenger_statsfilename",
		"",
		"[Optional] Provide a path to a file that will store the stats analyzer output at each interval. (for challenger host)")
	flag.StringVar(&challengerStatsFilename2,
		"challenger_statsfilename2",
		"",
		"[Optional] Provide a path to a file that will store the stats analyzer output at each interval. (for challenger2 host)")
	flag.StringVar(&challengerStatsFilename3,
		"challenger_statsfilename3",
		"",
		"[Optional] Provide a path to a file that will store the stats analyzer output at each interval. (for challenger3 host)")
	flag.StringVar(&opFilter,
		"op_filter",
		"",
		"[Optional] If specified, we'll only execute ops of that particular type")
}

func parseFlags() error {
	flag.Parse()
	validArgs := true
	errorMsg := ""

	if style == "" {
		validArgs = false
		errorMsg = "Missing `style` argument."
	} else if style != "stress" && style != "real" {
		validArgs = false
		errorMsg = "Invalid `style` argument passed to program: " + style + ". The only acceptable values are \"stress\" and \"real\"."
	} else if opsFilename == "" {
		validArgs = false
		errorMsg = "Missing required `ops_filename` argument."
	} else if workers <= 0 {
		validArgs = false
		errorMsg = "The `workers` argument must be a positive number."
	}

	if !validArgs {
		fmt.Println(errorMsg)
		fmt.Println("\nUsage:")
		flag.PrintDefaults()
		os.Exit(1)
	}

	var err error
	if logger, err = flashback.NewLogger(stdout, stderr); err != nil {
		return err
	}
	return nil
}

func makeOpsChan(style string, opsFilename string, logger *flashback.Logger) (chan *flashback.Op, error) {
	// Prepare to dispatch ops
	var (
		reader flashback.OpsReader
		err    error
	)

	if style == "real" && cyclic == true {
		reader = flashback.NewCyclicOpsReader(func() flashback.OpsReader {
			err, reader := flashback.NewFileByLineOpsReader(opsFilename, logger, opFilter)
			panicOnError(err)
			return reader
		}, logger)
	} else {
		err, reader = flashback.NewFileByLineOpsReader(opsFilename, logger, opFilter)
		if err != nil {
			return nil, err
		}
	}

	if startTime > 0 {
		if _, err := reader.SetStartTime(startTime); err != nil {
			return nil, err
		}
	}
	if numSkipOps > 0 {
		if err := reader.SkipOps(numSkipOps); err != nil {
			return nil, err
		}
	}

	if style == "stress" {
		return flashback.NewBestEffortOpsDispatcher(reader, maxOps, logger), nil
	} else {
		return flashback.NewByTimeOpsDispatcher(reader, maxOps, logger, speedup), nil
	}
}

type node struct {
	name          string
	url           string
	statsFile     *os.File
	statsChan     chan flashback.OpStat
	statsAnalyzer *flashback.StatsAnalyzer
}

type nodeWorkerState struct {
	name    string
	session *mgo.Session
	exec    *flashback.OpsExecutor
}

func main() {
	// Will enable system threads to make sure all cpus can be well utilized.
	runtime.GOMAXPROCS(runtime.NumCPU())
	err := parseFlags()
	panicOnError(err)
	defer logger.Close()

	opsChan, err := makeOpsChan(style, opsFilename, logger)
	panicOnError(err)

	createNode := func(name string, nodeUrl string, filename string) node {
		var n node
		// stats file
		if filename != "" {
			var err error
			n.statsFile, err = os.Create(filename)
			panicOnError(err)
		} else {
			n.statsFile = nil
		}
		n.name = name
		n.url = nodeUrl
		n.statsChan = make(chan flashback.OpStat, workers*100)
		n.statsAnalyzer = flashback.NewStatsAnalyzer(n.statsChan)
		return n
	}

	var nodes []node
	// create "default" node
	nodes = append(nodes, createNode("default", url, statsFilename))
	// create "challenger" node if necessary
	if challengerUrl != "" {
		nodes = append(nodes, createNode("challenger", challengerUrl, challengerStatsFilename))
	}
	// create "challenger2" node if necessary
	if challengerUrl2 != "" {
		nodes = append(nodes, createNode("challenger2", challengerUrl2, challengerStatsFilename2))
	}
	// create "challenger3" node if necessary
	if challengerUrl3 != "" {
		nodes = append(nodes, createNode("challenger3", challengerUrl3, challengerStatsFilename3))
	}

	// close stats files
	for _, n := range nodes {
		if n.statsFile != nil {
			defer n.statsFile.Close()
		}
	}

	// Set up workers to do the job
	exit := make(chan int)
	opsExecuted := int64(0)
	fetch := func(id int) {
		logger.Infof("Worker #%d report for duty\n", id)

		workerStates := make([]nodeWorkerState, len(nodes))

		for i, n := range nodes {
			session, err := mgo.Dial(n.url)
			panicOnError(err)
			session.SetSocketTimeout(time.Duration(socketTimeout))
			defer session.Close()
			workerStates[i] = nodeWorkerState{
				n.name,
				session,
				flashback.NewOpsExecutor(session, n.statsChan, logger),
			}
		}

		for {
			op := <-opsChan
			if op == nil {
				break
			}
			op = flashback.CanonicalizeOp(op)
			if op == nil {
				continue
			}

			var wg sync.WaitGroup
			wg.Add(len(nodes))

			execute := func(executor *flashback.OpsExecutor, name string) {
				defer wg.Done()
				err := executor.Execute(op)
				if err != nil {
					if verbose == true {
						logger.Error(fmt.Sprintf(
							"[%s] error executing op - type:%s,database:%s,collection:%s,error:%s", name,
							op.Type, op.Database, op.Collection, err))
					}
				}
			}

			for _, ws := range workerStates {
				go execute(ws.exec, ws.name)
			}
			wg.Wait()

			if slowOpThresholdMs > 0 {
				isSlow := func(latency time.Duration) bool {
					return latency > time.Duration(slowOpThresholdMs)*time.Millisecond
				}
				wasAnyOpSlow := false
				for _, ws := range workerStates {
					if isSlow(ws.exec.LastLatency()) {
						wasAnyOpSlow = true
						break
					}
				}
				if wasAnyOpSlow {
					var timeOutput string
					for _, ws := range workerStates {
						timeOutput = fmt.Sprintf("%s %v (%s)", timeOutput, ws.exec.LastLatency(), ws.name)
					}
					logger.Infof(fmt.Sprintf("Slow op - %s\ntype:%s,database:%s,collection:%s\n\t%v",
						timeOutput, op.Type, op.Database, op.Collection, op.Content))
				}
			}

			atomic.AddInt64(&opsExecuted, 1)
		}
		exit <- 1
		logger.Infof("Worker #%d done!\n", id)
	}

	for i := 0; i < workers; i++ {
		go fetch(i)
	}

	report := func() {
		printStatus := func(status *flashback.ExecutionStatus, statsOut *os.File, name string) {
			logger.Infof("[%s] Executed %d ops (%d in interval), got %d errors (%d in interval), "+
				"%.2f ops/sec (total), %.2f ops/sec (interval)", name, status.OpsExecuted, status.IntervalOpsExecuted,
				status.OpsErrors, status.IntervalOpsErrors, status.OpsPerSec, status.IntervalOpsPerSec)

			var statsLineOutput string
			if statsOut != nil {
				timestamp := time.Now().Format("2006-01-02 15:04:05 -0700")
				statsLineOutput = fmt.Sprintf("%s,%d,%.2f", timestamp, status.IntervalOpsExecuted, status.IntervalOpsPerSec)
			}

			for _, opType := range flashback.AllOpTypes {
				latencies := status.Latencies[opType]
				intervalLatencies := status.IntervalLatencies[opType]
				logger.Infof("  Op type: %s, count: %d, interval count %d, avg ops/sec: %.2f, interval ops/sec: %.2f",
					opType, status.Counts[opType], status.IntervalCounts[opType],
					status.TypeOpsSec[opType], status.IntervalTypeOpsSec[opType])
				template := "   %s: P50: %.2fms, P70: %.2fms, P90: %.2fms, P95 %.2fms, P99 %.2fms, Max %.2fms\n"
				logger.Infof(template, "Total", latencies[flashback.P50], latencies[flashback.P70], latencies[flashback.P90],
					latencies[flashback.P95], latencies[flashback.P99], status.MaxLatency[opType])
				logger.Infof(template, "Interval", intervalLatencies[flashback.P50], intervalLatencies[flashback.P70],
					intervalLatencies[flashback.P90], intervalLatencies[flashback.P95], intervalLatencies[flashback.P99],
					status.IntervalMaxLatency[opType])

				if statsOut != nil {
					statsLineOutput = fmt.Sprintf("%s,%d,%.2f", statsLineOutput,
						status.IntervalCounts[opType], status.IntervalTypeOpsSec[opType])
				}
			}

			// Write stats to disk at each interval for analysis later
			// Format is:
			// time,  ops, ops/sec, insert ops, inserts/sec, update ops, update/sec, remove ops, remove/sec,
			// query ops, query/sec, count ops, count/sec, fam ops, fam/sec
			if statsOut != nil {
				statsOut.WriteString(statsLineOutput + "\n")
			}
		}

		for _, n := range nodes {
			printStatus(n.statsAnalyzer.GetStatus(), n.statsFile, n.name)
		}
	}

	reportTicker := time.NewTicker(5 * time.Second)
	// Periodically report execution status
	go func() {
		for range reportTicker.C {
			report()
		}
	}()

	// Wait for workers
	received := 0
	for received < workers {
		<-exit
		received += 1
	}
	reportTicker.Stop()
	// report one last time
	report()
}
