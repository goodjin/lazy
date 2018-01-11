package main

import (
	"encoding/json"
	"fmt"
	"github.com/olivere/elastic"
	"log"
	"strings"
	"sync"
	"time"
)

type LogParserTask struct {
	LogConfig *LogSetting `json:"LogSetting"`
	sync.Mutex
	Config   []byte
	ID       string
	dataChan chan *elastic.BulkIndexRequest
	exitChan chan int
}

func (t *LogParserTask) Stop() {
	close(t.exitChan)
}

func (t *LogParserTask) GetID() string {
	return t.ID
}

func (t *LogParserTask) DetailInfo() []byte {
	return t.Config
}

func NewLogParserTask(id string, config []byte) *LogParserTask {
	logParserTask := &LogParserTask{}
	if err := json.Unmarshal(config, logParserTask); err != nil {
		log.Println("bad task config", err)
		return nil
	}
	logParserTask.Config = config
	logParserTask.ID = id
	logParserTask.exitChan = make(chan int)
	return logParserTask
}

func (t *LogParserTask) StartDataSource() error {
	var err error
	switch t.LogConfig.Config["Source"] {
	case "NSQ":
		t.dataChan = NewNSQTask(t.LogConfig)
		return err
	default:
		return fmt.Errorf("not supported")
	}
}

func (t *LogParserTask) StorageBackend() {
	switch t.LogConfig.Config["StorageBackend"] {
	case "Elastic":
		t.StartElastic()
	default:
		for {
			select {
			case <-t.dataChan:
			case <-t.exitChan:
				return
			}
		}
	}
}
func (t *LogParserTask) Start() error {
	err := t.StartDataSource()
	if err != nil {
		return err
	}
	go t.StorageBackend()
	return nil
}

func (t *LogParserTask) StartElastic() {
	hosts := strings.Split(t.LogConfig.Config["ElasticSearchEndPoint"], ",")
	c, err := elastic.NewClient(elastic.SetURL(hosts...))
	if err != nil {
		log.Println("create elastic client", err)
		return
	}
	logsource := t.LogConfig.Config["LogSource"]
	ticker := time.Tick(time.Second * 60)
	yy, mm, dd := time.Now().Date()
	indexPatten := fmt.Sprintf("-%d.%d.%d", yy, mm, dd)
	bulkProcessor, err := c.BulkProcessor().FlushInterval(10 * time.Second).Workers(t.LogConfig.TasksCount).After(t.afterFn).Do(nil)
	if err != nil {
		log.Println("create elastic processor and start", err)
		return
	}
	searchIndex := logsource + indexPatten
	for {
		select {
		case <-ticker:
			timestamp := time.Now()
			yy, mm, dd = timestamp.Date()
			indexPatten = fmt.Sprintf("-%d.%d.%d", yy, mm, dd)
			searchIndex = logsource + indexPatten
		case indexObject := <-t.dataChan:
			bulkProcessor.Add(indexObject.Index(searchIndex))
		case <-t.exitChan:
			bulkProcessor.Stop()
			log.Println("exit elasticsearch")
			return
		}
	}
}

func (m *LogParserTask) afterFn(executionID int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if err != nil {
		for _, request := range requests {
			m.dataChan <- &elastic.BulkIndexRequest{BulkableRequest: request}
		}
	}
}
