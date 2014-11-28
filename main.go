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
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	cfg "github.com/alyu/configparser"
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

func handleConnection(conn net.Conn, schemas []*StorageSchema, aggrs []*StorageAggregation) {
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

		elems[2] = strings.TrimRight(elems[2], "\n")
		tsf, err := strconv.ParseFloat(elems[2], 64)
		if err != nil {
			logger.Logf("invalid timestamp '%s': %s", elems[2], err.Error())
			continue
		}
		ts := int(tsf)

		if metric == "" || ts == 0 {
			logger.Logf("invalid line: %s", string(line))
			continue
		}

		logger.Logf("metric: %s, value: %f, ts: %d", metric, value, ts)

		// do what we want to do
		path := config.WhisperData + "/" + strings.Replace(metric, ".", "/", -1) + ".wsp"
		w, err := whisper.Open(path)
		if err != nil {
			var retentions whisper.Retentions
			for _, s := range schemas {
				retentions = s.retentions
				if s.pattern.MatchString(metric) {
					break
				}
			}

			// http://graphite.readthedocs.org/en/latest/config-carbon.html#storage-aggregation-conf
			var aggr whisper.AggregationMethod = whisper.Average
			var xfilesf float32 = 0.5
			for _, a := range aggrs {
				if a.pattern.MatchString(metric) {
					aggr = a.aggregationMethod
					xfilesf = float32(a.xFilesFactor)
					break
				}
			}
			logger.Logf("creating %s", path)
			w, err = whisper.Create(path, retentions, aggr, xfilesf)
			if err != nil {
				logger.Logf("failed to create new whisper file %s: %s",
					path, err.Error())
				continue
			}
		}

		w.Update(value, int(ts))
		w.Close()
	}
}

func listenAndServe(listen string, schemas []*StorageSchema, aggrs []*StorageAggregation) {
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
		go handleConnection(conn, schemas, aggrs)
	}
}

type StorageSchema struct {
	name       string
	pattern    *regexp.Regexp
	retentions whisper.Retentions
}

func readStorageSchemas(file string) ([]*StorageSchema, error) {
	config, err := cfg.Read(file)
	if err != nil {
		return nil, err
	}

	sections, err := config.AllSections()
	if err != nil {
		return nil, err
	}

	var ret []*StorageSchema
	for _, s := range sections {
		var sschema StorageSchema
		// this is mildly stupid, but I don't feel like forking
		// configparser just for this
		sschema.name =
			strings.Trim(strings.SplitN(s.String(), "\n", 2)[0], " []")
		sschema.pattern, err = regexp.Compile(s.ValueOf("pattern"))
		if err != nil {
			logger.Logf("failed to parse pattern '%s'for [%s]: %s",
				s.ValueOf("pattern"), sschema.name, err.Error())
			continue
		}
		sschema.retentions, err = whisper.ParseRetentionDefs(s.ValueOf("retentions"))

		ret = append(ret, &sschema)
	}

	return ret, nil
}

type StorageAggregation struct {
	name              string
	pattern           *regexp.Regexp
	xFilesFactor      float64
	aggregationMethod whisper.AggregationMethod
}

func readStorageAggregations(file string) ([]*StorageAggregation, error) {
	config, err := cfg.Read(file)
	if err != nil {
		return nil, err
	}

	sections, err := config.AllSections()
	if err != nil {
		return nil, err
	}

	var ret []*StorageAggregation
	for _, s := range sections {
		var saggr StorageAggregation
		// this is mildly stupid, but I don't feel like forking
		// configparser just for this
		saggr.name =
			strings.Trim(strings.SplitN(s.String(), "\n", 2)[0], " []")
		saggr.pattern, err = regexp.Compile(s.ValueOf("pattern"))
		if err != nil {
			logger.Logf("failed to parse pattern '%s'for [%s]: %s",
				s.ValueOf("pattern"), saggr.name, err.Error())
			continue
		}
		saggr.xFilesFactor, err = strconv.ParseFloat(s.ValueOf("xFilesFactor"), 64)
		if err != nil {
			logger.Logf("failed to parse xFilesFactor '%s': %s",
				s.ValueOf("xFilesFactor"), err.Error())
			continue
		}

		switch s.ValueOf("aggregationMethod") {
		case "average", "avg":
			saggr.aggregationMethod = whisper.Average
		case "sum":
			saggr.aggregationMethod = whisper.Sum
		case "last":
			saggr.aggregationMethod = whisper.Last
		case "max":
			saggr.aggregationMethod = whisper.Max
		case "min":
			saggr.aggregationMethod = whisper.Min
		default:
			logger.Logf("unknown aggregation method '%s'",
				s.ValueOf("aggregationMethod"))
			continue
		}

		ret = append(ret, &saggr)
	}

	return ret, nil
}

func main() {
	port := flag.Int("p", 2003, "port to bind to")
	reportport := flag.Int("reportport", 8080, "port to bind http report interface to")
	verbose := flag.Bool("v", false, "enable verbose logging")
	debug := flag.Bool("vv", false, "enable more verbose (debug) logging")
	whisperdata := flag.String("w", config.WhisperData, "location where whisper files are stored")
	maxprocs := flag.Int("maxprocs", runtime.NumCPU()*80/100, "GOMAXPROCS")
	logdir := flag.String("logdir", "/var/log/carbonwriter/", "logging directory")
	schemafile := flag.String("schemafile", "/etc/carbon/storage-schemas.conf", "storage-schemas.conf location")
	aggrfile := flag.String("aggrfile", "/etc/carbon/storage-aggregation.conf", "storage-aggregation.conf location")
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

	schemas, err := readStorageSchemas(*schemafile)
	if err != nil {
		logger.Logf("failed to read %s: %s", schemafile, err.Error())
		os.Exit(1)
	}

	aggrs, err := readStorageAggregations(*aggrfile)
	if err != nil {
		logger.Logf("failed to read %s: %s", schemafile, err.Error())
		os.Exit(1)
	}

	config.WhisperData = strings.TrimRight(*whisperdata, "/")
	logger.Logf("writing whisper files to: %s", config.WhisperData)
	logger.Logf("reading storage schemas from: %s", schemafile)
	logger.Logf("reading aggregation rules from: %s", aggrfile)

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
	go listenAndServe(listen, schemas, aggrs)
	err = http.ListenAndServe(httplisten, nil)
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
