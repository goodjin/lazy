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
// }
type ElasticSearchWriter struct {
	IndexPerfix  string
	tasksCount   int
	Type         string
	esClient     *elasticsearch.Client
	es7Client    *elasticsearch7.Client
	BulkCount    int
	FlushTimeout bool
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
	hosts := strings.Split(config["ElasticSearchEndPoint"], ",")
	es := &ElasticSearchWriter{IndexPerfix: config["IndexPerfix"]}
	es.BulkCount = 100
	cfg := elasticsearch.Config{
		Addresses: hosts,
	}
	var err error
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
	hystrix.ConfigureCommand("BulkInsert", hystrix.CommandConfig{
		Timeout:               1000,
		MaxConcurrentRequests: 10,
		ErrorPercentThreshold: 25,
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
	flushticker := time.Tick(time.Second * 5)
	yy, mm, dd := time.Now().Date()
	indexName := fmt.Sprintf("%s-%d.%d.%d", es.IndexPerfix, yy, mm, dd)
	var buf bytes.Buffer
	count := 0
	meta := []byte(fmt.Sprintf(`{"index":{"_index":"%s","_type":"%s"}}%s`, indexName, es.Type, "\n"))
	var raw map[string]interface{}
	for {
		select {
		case <-ticker:
			yy, mm, dd := time.Now().Date()
			indexName = fmt.Sprintf("%s-%d.%d.%d", es.IndexPerfix, yy, mm, dd)
			meta = []byte(fmt.Sprintf(`{"index":{"_index":"%s","_type":"%s"}}%s`, indexName, es.Type, "\n"))
			//es.Stats()
		case <-flushticker:
			es.FlushTimeout = true
		case msg := <-dataChan:
			data, _ := json.Marshal(msg)
			buf.Grow(len(meta) + len(data) + 1)
			buf.Write(meta)
			buf.Write(data)
			buf.Write([]byte("\n"))
			count++
		retry:
			err := hystrix.Do("BulkInsert", func() error {
				if count > es.BulkCount || es.FlushTimeout {
					switch es.esVersion {
					case 6:
						res, err := es.esClient.Bulk(bytes.NewReader(buf.Bytes()))
						if err != nil {
							es.metricstatus.WithLabelValues("Failed").Add(float64(es.BulkCount))
							log.Println(err)
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
					es.FlushTimeout = false
					buf.Reset()
					es.metricstatus.WithLabelValues("Flushed").Inc()
				}
				return nil
			}, nil)
			if err != nil {
				goto retry
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
