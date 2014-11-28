package main

import (
	"bufio"
	"expvar"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dgryski/httputil"
	whisper "github.com/grobian/go-whisper"
	"github.com/lestrrat/go-file-rotatelogs"
	g2g "github.com/peterbourgon/g2g"
)

var config = struct {
	WhisperData  string
	GraphiteHost string
	MaxGlobs     int
	Buckets      int
}{
	WhisperData: "/var/lib/carbon/whisper",
	MaxGlobs:    10,
	Buckets:     10,
}

// grouped expvars for /debug/vars and graphite
var Metrics = struct {
	RenderRequests *expvar.Int
	RenderErrors   *expvar.Int
	NotFound       *expvar.Int
	FindRequests   *expvar.Int
	FindErrors     *expvar.Int
	FindZero       *expvar.Int
	InfoRequests   *expvar.Int
	InfoErrors     *expvar.Int
}{
	RenderRequests: expvar.NewInt("render_requests"),
	RenderErrors:   expvar.NewInt("render_errors"),
	NotFound:       expvar.NewInt("notfound"),
	FindRequests:   expvar.NewInt("find_requests"),
	FindErrors:     expvar.NewInt("find_errors"),
	FindZero:       expvar.NewInt("find_zero"),
	InfoRequests:   expvar.NewInt("info_requests"),
	InfoErrors:     expvar.NewInt("info_errors"),
}

var BuildVersion string = "(development build)"

var logger logLevel

func handleConnection(conn net.Conn) {
	bufconn := bufio.NewReader(conn)

	for {
		line, err := bufconn.ReadBytes('\n')
		if err != nil {
			conn.Close()
			if err != io.EOF {
				logger.Logf("read failed: %s", err.Error())
			}
			break
		}

		elems := strings.Split(string(line), " ")
		if len(elems) != 3 {
			logger.Logf("invalid line: %s", string(line))
			continue
		}

		metric := elems[0]

		value, err := strconv.ParseFloat(elems[1], 64)
		if err != nil {
			logger.Logf("invalue value '%s': %s", elems[1], err.Error())
			continue
		}

		ts, err := strconv.ParseInt(strings.TrimRight(elems[2], "\n"), 10, 32)
		if err != nil {
			logger.Logf("invalid timestamp '%s': %s", elems[2], err.Error())
			continue
		}

		if metric == "" || ts == 0 {
			logger.Logf("invalid line: %s", string(line))
			continue
		}

		logger.Logf("metric: %s, value: %f, ts: %d", metric, value, ts)

		// do what we want to do
		path := config.WhisperData + "/" + strings.Replace(metric, ".", "/", -1) + ".wsp"
		w, err := whisper.Open(path)
		if err != nil {
			// TODO: create a new metric
			continue // for the time being
		}

		w.Update(value, int(ts))
		w.Close()
	}
}

func listenAndServe(listen string) {
	l, err := net.Listen("tcp", listen)
	if err != nil {
		logger.Logf("failed to listen on %s: %s", listen, err.Error())
		os.Exit(1)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			logger.Logf("failed to accept connection: %s", err.Error())
			continue
		}
		go handleConnection(conn)
	}
}

func main() {
	port := flag.Int("p", 2003, "port to bind to")
	reportport := flag.Int("reportport", 8080, "port to bind http report interface to")
	verbose := flag.Bool("v", false, "enable verbose logging")
	debug := flag.Bool("vv", false, "enable more verbose (debug) logging")
	whisperdata := flag.String("w", config.WhisperData, "location where whisper files are stored")
	maxprocs := flag.Int("maxprocs", runtime.NumCPU()*80/100, "GOMAXPROCS")
	logdir := flag.String("logdir", "/var/log/carbonwriter/", "logging directory")
	logtostdout := flag.Bool("stdout", false, "log also to stdout")

	flag.Parse()

	rl := rotatelogs.NewRotateLogs(
		*logdir + "/carbonwriter.%Y%m%d%H%M.log",
	)

	// Optional fields must be set afterwards
	rl.LinkName = *logdir + "/carbonwriter.log"

	if *logtostdout {
		log.SetOutput(io.MultiWriter(os.Stdout, rl))
	} else {
		log.SetOutput(rl)
	}

	expvar.NewString("BuildVersion").Set(BuildVersion)
	log.Println("starting carbonwriter", BuildVersion)

	loglevel := LOG_NORMAL
	if *verbose {
		loglevel = LOG_DEBUG
	}
	if *debug {
		loglevel = LOG_TRACE
	}

	logger = logLevel(loglevel)

	config.WhisperData = strings.TrimRight(*whisperdata, "/")
	logger.Logf("writing whisper files from: %s", config.WhisperData)

	runtime.GOMAXPROCS(*maxprocs)
	logger.Logf("set GOMAXPROCS=%d", *maxprocs)

	httputil.PublishTrackedConnections("httptrack")
	expvar.Publish("requestBuckets", expvar.Func(renderTimeBuckets))

	// +1 to track every over the number of buckets we track
	timeBuckets = make([]int64, config.Buckets+1)

	// nothing in the config? check the environment
	if config.GraphiteHost == "" {
		if host := os.Getenv("GRAPHITEHOST") + ":" + os.Getenv("GRAPHITEPORT"); host != ":" {
			config.GraphiteHost = host
		}
	}

	// only register g2g if we have a graphite host
	if config.GraphiteHost != "" {

		logger.Logf("Using graphite host %v", config.GraphiteHost)

		// register our metrics with graphite
		graphite, err := g2g.NewGraphite(config.GraphiteHost, 60*time.Second, 10*time.Second)
		if err != nil {
			log.Fatalf("unable to connect to to graphite: %v: %v", config.GraphiteHost, err)
		}

		hostname, _ := os.Hostname()
		hostname = strings.Replace(hostname, ".", "_", -1)

		//		graphite.Register(fmt.Sprintf("carbon.writer.%s.metricsReceived",
		//			hostname), Metrics.received)

		for i := 0; i <= config.Buckets; i++ {
			graphite.Register(fmt.Sprintf("carbon.writer.%s.write_in_%dms_to_%dms", hostname, i*100, (i+1)*100), bucketEntry(i))
		}
	}

	listen := fmt.Sprintf(":%d", *port)
	httplisten := fmt.Sprintf(":%d", *reportport)
	logger.Logf("listening on %s, statistics via %s", listen, httplisten)
	go listenAndServe(listen)
	err := http.ListenAndServe(httplisten, nil)
	if err != nil {
		log.Fatalf("%s", err)
	}
	logger.Logf("stopped")
}

type logLevel int

const (
	LOG_NORMAL logLevel = iota
	LOG_DEBUG
	LOG_TRACE
)

func (ll logLevel) Debugf(format string, a ...interface{}) {
	if ll >= LOG_DEBUG {
		log.Printf(format, a...)
	}
}

func (ll logLevel) Debugln(a ...interface{}) {
	if ll >= LOG_DEBUG {
		log.Println(a...)
	}
}

func (ll logLevel) Tracef(format string, a ...interface{}) {
	if ll >= LOG_TRACE {
		log.Printf(format, a...)
	}
}

func (ll logLevel) Traceln(a ...interface{}) {
	if ll >= LOG_TRACE {
		log.Println(a...)
	}
}
func (ll logLevel) Logln(a ...interface{}) {
	log.Println(a...)
}

func (ll logLevel) Logf(format string, a ...interface{}) {
	log.Printf(format, a...)
}

var timeBuckets []int64

type bucketEntry int

func (b bucketEntry) String() string {
	return strconv.Itoa(int(atomic.LoadInt64(&timeBuckets[b])))
}

func renderTimeBuckets() interface{} {
	return timeBuckets
}

func bucketRequestTimes(req *http.Request, t time.Duration) {

	ms := t.Nanoseconds() / int64(time.Millisecond)

	bucket := int(math.Log(float64(ms)) * math.Log10E)

	if bucket < 0 {
		bucket = 0
	}

	if bucket < config.Buckets {
		atomic.AddInt64(&timeBuckets[bucket], 1)
	} else {
		// Too big? Increment overflow bucket and log
		atomic.AddInt64(&timeBuckets[config.Buckets], 1)
		logger.Logf("Slow Request: %s: %s", t.String(), req.URL.String())
	}
}
