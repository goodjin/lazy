package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/olivere/elastic"
	"github.com/prometheus/client_golang/prometheus"
)

// config
// {
// "IndexPerfix":"xxxx",
// "TaskCount":"10",
// "IndexType":"syslog",
// "Type":"elasticsearch"
// }
type ElasticSearchWriter struct {
	IndexPerfix   string
	tasksCount    int
	Type          string
	bulkProcessor *elastic.BulkProcessor
	exitChan      chan int
	metricstatus  *prometheus.GaugeVec
}

func NewElasitcSearchWriter(config map[string]string) (*ElasticSearchWriter, error) {
	hosts := strings.Split(config["ElasticSearchEndPoint"], ",")
	client, err := elastic.NewClient(elastic.SetURL(hosts...))
	if err != nil {
		log.Println("create elastic client", err)
		return nil, err
	}
	fmt.Println("Start elasticsearch writer")
	es := &ElasticSearchWriter{IndexPerfix: config["IndexPerfix"]}
	es.tasksCount, err = strconv.Atoi(config["TaskCount"])
	if err != nil {
		es.tasksCount = 5
	}
	es.exitChan = make(chan int)
	es.Type = config["IndexType"]
	es.bulkProcessor, err = client.BulkProcessor().FlushInterval(10 * time.Second).Workers(es.tasksCount).After(es.afterFn).Stats(true).Do(context.Background())
	es.metricstatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "elasticsearch",
			Subsystem: "bulk_processor",
			Name:      config["Taskname"],
			Help:      "elsticsearch producer status.",
		},
		[]string{"opt"},
	)
	// Register status
	prometheus.Register(es.metricstatus)
	return es, err
}

func (es *ElasticSearchWriter) afterFn(executionID int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if err != nil {
		for _, request := range requests {
			es.bulkProcessor.Add(request)
		}
	}
}
func (es *ElasticSearchWriter) Stop() {
	prometheus.Unregister(es.metricstatus)
	close(es.exitChan)
}

func (es *ElasticSearchWriter) Start(dataChan chan *map[string]interface{}) {
	ticker := time.Tick(time.Second * 60)
	yy, mm, dd := time.Now().Date()
	indexName := fmt.Sprintf("%s-%d.%d.%d", es.IndexPerfix, yy, mm, dd)
	for {
		select {
		case <-ticker:
			yy, mm, dd := time.Now().Date()
			indexName = fmt.Sprintf("%s-%d.%d.%d", es.IndexPerfix, yy, mm, dd)
			es.Stats()
		case msg := <-dataChan:
			indexObject := elastic.NewBulkIndexRequest().Doc(msg).Type(es.Type)
			es.bulkProcessor.Add(indexObject.Index(indexName))
		case <-es.exitChan:
			es.bulkProcessor.Stop()
			log.Println("exit elasticsearch")
			return
		}
	}
}

func (es *ElasticSearchWriter) Stats() {
	stats := es.bulkProcessor.Stats()
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
