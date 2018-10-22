package main

import (
	"context"
	"fmt"
	"github.com/go-kit/kit/metrics/statsd"
	"github.com/olivere/elastic"
	"log"
	"strconv"
	"strings"
	"time"
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
	dataChan      chan *map[string]interface{}
	statsd        *statsd.Statsd
	exitChan      chan int
}

func NewElasitcSearchWriter(config map[string]string) (*ElasticSearchWriter, error) {
	hosts := strings.Split(config["ElasticSearchEndPoint"], ",")
	client, err := elastic.NewClient(elastic.SetURL(hosts...))
	if err != nil {
		log.Println("create elastic client", err)
		return nil, err
	}
	es := &ElasticSearchWriter{IndexPerfix: config["IndexPerfix"]}
	es.tasksCount, err = strconv.Atoi(config["TaskCount"])
	if err != nil {
		es.tasksCount = 5
	}
	es.Type = config["IndexType"]
	es.bulkProcessor, err = client.BulkProcessor().FlushInterval(10 * time.Second).Workers(es.tasksCount).After(es.afterFn).Stats(true).Do(context.Background())
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
	close(es.exitChan)
}

func (es *ElasticSearchWriter) Start(dataChan chan *map[string]interface{}) {
	ticker := time.Tick(time.Second * 60)
	yy, mm, dd := time.Now().Date()
	indexName := fmt.Sprintf("%s-%d.%d.%d", es.IndexPerfix, yy, mm, dd)
	go func() {
		for {
			select {
			case <-ticker:
				yy, mm, dd := time.Now().Date()
				indexName = fmt.Sprintf("%s-%d.%d.%d", es.IndexPerfix, yy, mm, dd)
			case msg := <-dataChan:
				indexObject := elastic.NewBulkIndexRequest().Doc(msg).Type(es.Type)
				es.bulkProcessor.Add(indexObject.Index(indexName))
			case <-es.exitChan:
				es.bulkProcessor.Stop()
				log.Println("exit elasticsearch")
				return
			}
		}
	}()
}

func (es *ElasticSearchWriter) Stats() map[string]int64 {
	stats := es.bulkProcessor.Stats()
	rst := make(map[string]int64)
	rst["Flushed"] = stats.Flushed
	rst["Committed"] = stats.Committed
	rst["Indexed"] = stats.Indexed
	rst["Created"] = stats.Created
	rst["Updated"] = stats.Updated
	rst["Deleted"] = stats.Deleted
	rst["Succeeded"] = stats.Succeeded
	rst["Failed"] = stats.Failed
	for i, w := range stats.Workers {
		rst[fmt.Sprintf("worker%d_queued", i)] = w.Queued
	}
	return rst
}
func (es *ElasticSearchWriter) SetStatsd(statsd *statsd.Statsd) {
	es.statsd = statsd
}
