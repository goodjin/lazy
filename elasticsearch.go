package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/afex/hystrix-go/hystrix"
	elasticsearch "github.com/elastic/go-elasticsearch/v6"
	elasticsearch7 "github.com/elastic/go-elasticsearch/v7"
	"github.com/prometheus/client_golang/prometheus"
)

// ElasticSearchWriter write method for elasticsearch
// config
// {
// "IndexPerfix":"xxxx",
// "TaskCount":"10",
// "IndexType":"syslog",
// "Type":"elasticsearch"
// "FlushTimeout":"5",
// "BulkCount":"100",
// "Timeout":"10000",
// "RequestVolumeThreshold":"20000",
// "MaxConcurrentRequests":"100",
// "ErrorPercentThreshold":"25",
// }
type ElasticSearchWriter struct {
	IndexPerfix  string
	tasksCount   int
	Type         string
	esClient     *elasticsearch.Client
	es7Client    *elasticsearch7.Client
	BulkCount    int
	FlushTimeout int
	esVersion    int
	exitChan     chan int
	metricstatus *prometheus.CounterVec
}

/*
{
  "name" : "ops-elk1.yq.163.org-node0",
  "cluster_name" : "elk-syslog",
  "cluster_uuid" : "OOy6hPRFSt-PlLNJ26j8Ow",
  "version" : {
    "number" : "6.5.1",
    "build_flavor" : "default",
    "build_type" : "deb",
    "build_hash" : "8c58350",
    "build_date" : "2018-11-16T02:22:42.182257Z",
    "build_snapshot" : false,
    "lucene_version" : "7.5.0",
    "minimum_wire_compatibility_version" : "5.6.0",
    "minimum_index_compatibility_version" : "5.0.0"
  },
  "tagline" : "You Know, for Search"
}
*/

// NewElasitcSearchWriter create new object
func NewElasitcSearchWriter(config map[string]string) (*ElasticSearchWriter, error) {
	var err error
	hosts := strings.Split(config["ElasticSearchEndPoint"], ",")
	es := &ElasticSearchWriter{IndexPerfix: config["IndexPerfix"]}
	if len(config["FlushTimeout"]) < 1 {
		config["FlushTimeout"] = "5"
	}
	if len(config["BulkCount"]) < 1 {
		config["BulkCount"] = "100"
	}
	es.FlushTimeout, err = strconv.Atoi(config["FlushTimeout"])
	if err != nil {
		es.FlushTimeout = 5
	}
	es.BulkCount, err = strconv.Atoi(config["BulkCount"])
	if err != nil {
		es.BulkCount = 100
	}
	cfg := elasticsearch.Config{
		Addresses: hosts,
	}
	es.esClient, err = elasticsearch.NewClient(cfg)
	if err != nil {
		log.Println("create elastic client", err)
		return nil, err
	}
	res, err := es.esClient.Info()
	if err != nil {
		log.Printf("Error getting response: %s", err)
		return es, err
	}
	if res.IsError() {
		log.Printf("Error: %s", res.String())
	}
	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Printf("Error parsing the response body: %s", err)
	}
	es.esVersion = 6
	if r["version"].(map[string]interface{})["number"] == '7' {
		es.esVersion = 7
		cfg := elasticsearch7.Config{
			Addresses: hosts,
		}
		es.es7Client, err = elasticsearch7.NewClient(cfg)
	}
	fmt.Println("Start elasticsearch writer")
	es.tasksCount, err = strconv.Atoi(config["TaskCount"])
	if err != nil {
		es.tasksCount = 5
	}
	es.exitChan = make(chan int)
	es.Type = config["IndexType"]
	es.metricstatus = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "elasticsearch",
			Name:      fmt.Sprintf("bulk_processor_%s", config["Taskname"]),
			Help:      "elsticsearch producer status.",
		},
		[]string{"opt"},
	)
	if len(config["Timeout"]) < 1 {
		config["Timeout"] = "5000"
	}
	if len(config["RequestVolumeThreshold"]) < 1 {
		config["RequestVolumeThreshold"] = "20000"
	}
	if len(config["MaxConcurrentRequests"]) < 1 {
		config["MaxConcurrentRequests"] = "64"
	}
	if len(config["ErrorPercentThreshold"]) < 1 {
		config["ErrorPercentThreshold"] = "25"
	}

	timeout, err := strconv.Atoi(config["Timeout"])
	if timeout < 1000 {
		timeout = 1000
	}
	requestVolumeThreshold, err := strconv.Atoi(config["RequestVolumeThreshold"])
	if requestVolumeThreshold < 1 {
		requestVolumeThreshold = 20000
	}
	maxConcurrentRequests, err := strconv.Atoi(config["MaxConcurrentRequests"])
	if maxConcurrentRequests < 1 {
		maxConcurrentRequests = 100
	}
	errorPercentThreshold, err := strconv.Atoi(config["ErrorPercentThreshold"])
	if errorPercentThreshold < 1 {
		errorPercentThreshold = 25
	}
	hystrix.ConfigureCommand(fmt.Sprintf("%s_BulkInsert", es.IndexPerfix), hystrix.CommandConfig{
		Timeout:                timeout,
		RequestVolumeThreshold: requestVolumeThreshold,
		MaxConcurrentRequests:  maxConcurrentRequests,
		ErrorPercentThreshold:  errorPercentThreshold,
	})
	// Register status
	prometheus.Register(es.metricstatus)
	return es, err
}

// Stop stop all
func (es *ElasticSearchWriter) Stop() {
	prometheus.Unregister(es.metricstatus)
	close(es.exitChan)
}

// Start start read/insert data
func (es *ElasticSearchWriter) Start(dataChan chan *map[string]interface{}) {
	ticker := time.Tick(time.Second * 60)
	flushticker := time.Tick(time.Second * time.Duration(es.FlushTimeout))
	yy, mm, dd := time.Now().Date()
	indexName := fmt.Sprintf("%s-%d.%d.%d", es.IndexPerfix, yy, mm, dd)
	var buf bytes.Buffer
	count := 0
	meta := []byte(fmt.Sprintf(`{"index":{"_index":"%s","_type":"%s"}}%s`, indexName, es.Type, "\n"))
	var raw map[string]interface{}
	var flushTimeout bool
	for {
		select {
		case <-ticker:
			yy, mm, dd := time.Now().Date()
			indexName = fmt.Sprintf("%s-%d.%d.%d", es.IndexPerfix, yy, mm, dd)
			meta = []byte(fmt.Sprintf(`{"index":{"_index":"%s","_type":"%s"}}%s`, indexName, es.Type, "\n"))
			//es.Stats()
		case <-flushticker:
			flushTimeout = true
		case msg := <-dataChan:
			data, _ := json.Marshal(msg)
			buf.Grow(len(meta) + len(data) + 1)
			buf.Write(meta)
			buf.Write(data)
			buf.Write([]byte("\n"))
			count++
		retry:
			if count > es.BulkCount || flushTimeout {
				err := hystrix.Do(fmt.Sprintf("%s_BulkInsert", es.IndexPerfix), func() error {
					switch es.esVersion {
					case 6:
						res, err := es.esClient.Bulk(bytes.NewReader(buf.Bytes()))
						if err != nil {
							es.metricstatus.WithLabelValues("Failed").Add(float64(es.BulkCount))
							return err
						}
						if res.IsError() {
							if err = json.NewDecoder(res.Body).Decode(&raw); err == nil {
								log.Printf("  Error: [%d] %s: %s",
									res.StatusCode,
									raw["error"].(map[string]interface{})["type"],
									raw["error"].(map[string]interface{})["reason"],
								)
							}
							res.Body.Close()
							es.metricstatus.WithLabelValues("Failed").Add(float64(es.BulkCount))
							time.Sleep(time.Second)
							return fmt.Errorf("%s", raw["error"].(map[string]interface{})["reason"])
						}
						es.metricstatus.WithLabelValues("Indexed").Add(float64(es.BulkCount))
						res.Body.Close()
					case 7:
						res, err := es.es7Client.Bulk(bytes.NewReader(buf.Bytes()))
						if err != nil {
							es.metricstatus.WithLabelValues("Failed").Add(float64(es.BulkCount))
							return err
						}
						if res.IsError() {
							if err = json.NewDecoder(res.Body).Decode(&raw); err == nil {
								log.Printf("  Error: [%d] %s: %s",
									res.StatusCode,
									raw["error"].(map[string]interface{})["type"],
									raw["error"].(map[string]interface{})["reason"],
								)
							}
							res.Body.Close()
							es.metricstatus.WithLabelValues("Failed").Add(float64(es.BulkCount))
							time.Sleep(time.Second)
							return fmt.Errorf("%s", raw["error"].(map[string]interface{})["reason"])
						}
						es.metricstatus.WithLabelValues("Indexed").Add(float64(es.BulkCount))
						res.Body.Close()
					default:
					}
					count = 0
					flushTimeout = false
					buf.Reset()
					es.metricstatus.WithLabelValues("Flushed").Inc()
					return nil
				}, nil)
				if err != nil {
					time.Sleep(time.Second)
					goto retry
				}
			}
		case <-es.exitChan:
			log.Println("exit elasticsearch")
			return
		}
	}
}

/*
// Stats
func (es *ElasticSearchWriter) Stats() {
	stats := es.esClient.Metrics()
	es.metricstatus.WithLabelValues("Flushed").Set(float64(stats.Flushed))
	es.metricstatus.WithLabelValues("Committed").Set(float64(stats.Committed))
	es.metricstatus.WithLabelValues("Indexed").Set(float64(stats.Indexed))
	es.metricstatus.WithLabelValues("Created").Set(float64(stats.Created))
	es.metricstatus.WithLabelValues("Updated").Set(float64(stats.Updated))
	es.metricstatus.WithLabelValues("Deleted").Set(float64(stats.Deleted))
	es.metricstatus.WithLabelValues("Succeeded").Set(float64(stats.Succeeded))
	es.metricstatus.WithLabelValues("Failed").Set(float64(stats.Failed))
	for i, w := range stats.Workers {
		es.metricstatus.WithLabelValues(fmt.Sprintf("worker%d_queued", i)).Set(float64(w.Queued))
	}
}
*/
