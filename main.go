package main

import (
	"bufio"
	"expvar"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	cfg "github.com/alyu/configparser"
	"github.com/dgryski/carbonzipper/mlog"
	"github.com/dgryski/httputil"
	whisper "github.com/grobian/go-whisper"
	g2g "github.com/peterbourgon/g2g"
)

var config = struct {
	WhisperData  string
	GraphiteHost string
}{
	WhisperData: "/var/lib/carbon/whisper",
}

// grouped expvars for /debug/vars and graphite
var Metrics = struct {
	MetricsReceived *expvar.Int
}{
	MetricsReceived: expvar.NewInt("metrics_received"),
}

var BuildVersion string = "(development build)"

var logger mlog.Level

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

		if metric == "" {
			logger.Logf("invalid line: %s", string(line))
			continue
		}

		if ts == 0 {
			logger.Logf("invalid timestamp (0): %s", string(line))
			continue
		}

		logger.Debugf("metric: %s, value: %f, ts: %d", metric, value, ts)

		// catch panics from whisper-go library
		defer func() {
			if r := recover(); r != nil {
				logger.Logf("recovering from whisper panic (metric: %s): %v", metric, r)
				err := conn.Close()
				if err != nil {
					logger.Logf("error while closing connection after whisper panic: %v", err)
				}
			}
		}()

		// do what we want to do
		path := config.WhisperData + "/" + strings.Replace(metric, ".", "/", -1) + ".wsp"
		w, err := whisper.Open(path)
		if err != nil {
			var schema *StorageSchema = nil
			for _, s := range schemas {
				if s.pattern.MatchString(metric) {
					schema = s
					break
				}
			}
			if schema == nil {
				logger.Logf("no storage schema defined for %s", metric)
				continue
			}
			logger.Debugf("%s: found schema: %s", metric, schema.name)

			var aggr *StorageAggregation = nil
			for _, a := range aggrs {
				if a.pattern.MatchString(metric) {
					aggr = a
					break
				}
			}

			// http://graphite.readthedocs.org/en/latest/config-carbon.html#storage-aggregation-conf
			aggrName := "(default)"
			aggrStr := "average"
			aggrType := whisper.Average
			xfilesf := float32(0.5)
			if aggr != nil {
				aggrName = aggr.name
				aggrStr = aggr.aggregationMethodStr
				aggrType = aggr.aggregationMethod
				xfilesf = float32(aggr.xFilesFactor)
			}

			logger.Logf("creating %s: %s, retention: %s (section %s), aggregationMethod: %s, xFilesFactor: %f (section %s)",
				metric, path, schema.retentionStr, schema.name,
				aggrStr, xfilesf, aggrName)

			// whisper.Create doesn't mkdir, so let's do it ourself
			lastslash := strings.LastIndex(path, "/")
			if lastslash != -1 {
				os.MkdirAll(path[0:lastslash], os.ModeDir|os.ModePerm)
			}
			w, err = whisper.Create(path, schema.retentions, aggrType, xfilesf)
			if err != nil {
				logger.Logf("failed to create new whisper file %s: %s",
					path, err.Error())
				continue
			}
		}

		w.Update(value, int(ts))
		w.Close()

		Metrics.MetricsReceived.Add(1)
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
	name         string
	pattern      *regexp.Regexp
	retentionStr string
	retentions   whisper.Retentions
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
		if sschema.name == "" {
			continue
		}
		sschema.pattern, err = regexp.Compile(s.ValueOf("pattern"))
		if err != nil {
			logger.Logf("failed to parse pattern '%s'for [%s]: %s",
				s.ValueOf("pattern"), sschema.name, err.Error())
			continue
		}
		sschema.retentionStr = s.ValueOf("retentions")
		sschema.retentions, err = whisper.ParseRetentionDefs(sschema.retentionStr)
		logger.Debugf("adding schema [%s] pattern = %s retentions = %s",
			sschema.name, s.ValueOf("pattern"), sschema.retentionStr)

		ret = append(ret, &sschema)
	}

	return ret, nil
}

type StorageAggregation struct {
	name                 string
	pattern              *regexp.Regexp
	xFilesFactor         float64
	aggregationMethodStr string
	aggregationMethod    whisper.AggregationMethod
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
		if saggr.name == "" {
			continue
		}
		saggr.pattern, err = regexp.Compile(s.ValueOf("pattern"))
		if err != nil {
			logger.Logf("failed to parse pattern '%s'for [%s]: %s",
				s.ValueOf("pattern"), saggr.name, err.Error())
			continue
		}
		saggr.xFilesFactor, err = strconv.ParseFloat(s.ValueOf("xFilesFactor"), 64)
		if err != nil {
			logger.Logf("failed to parse xFilesFactor '%s' in %s: %s",
				s.ValueOf("xFilesFactor"), saggr.name, err.Error())
			continue
		}

		saggr.aggregationMethodStr = s.ValueOf("aggregationMethod")
		switch saggr.aggregationMethodStr {
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

		logger.Debugf("adding aggregation [%s] pattern = %s aggregationMethod = %s xFilesFactor = %f",
			saggr.name, s.ValueOf("pattern"),
			saggr.aggregationMethodStr, saggr.xFilesFactor)
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

	mlog.SetOutput(*logdir, "carbonwriter", *logtostdout)

	expvar.NewString("BuildVersion").Set(BuildVersion)
	log.Println("starting carbonwriter", BuildVersion)

	loglevel := mlog.Normal
	if *verbose {
		loglevel = mlog.Debug
	}
	if *debug {
		loglevel = mlog.Trace
	}

	logger = mlog.Level(loglevel)

	schemas, err := readStorageSchemas(*schemafile)
	if err != nil {
		logger.Logf("failed to read %s: %s", *schemafile, err.Error())
		os.Exit(1)
	}

	aggrs, err := readStorageAggregations(*aggrfile)
	if err != nil {
		logger.Logf("failed to read %s: %s", *aggrfile, err.Error())
		os.Exit(1)
	}

	config.WhisperData = strings.TrimRight(*whisperdata, "/")
	logger.Logf("writing whisper files to: %s", config.WhisperData)
	logger.Logf("reading storage schemas from: %s", *schemafile)
	logger.Logf("reading aggregation rules from: %s", *aggrfile)

	runtime.GOMAXPROCS(*maxprocs)
	logger.Logf("set GOMAXPROCS=%d", *maxprocs)

	httputil.PublishTrackedConnections("httptrack")

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

		graphite.Register(fmt.Sprintf("carbon.writer.%s.metricsReceived", hostname), Metrics.MetricsReceived)
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
