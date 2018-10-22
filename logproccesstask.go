package main

import (
	"encoding/json"
	"fmt"
	kitlog "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/metrics/statsd"
	"log"
	"os"
	"sync"
	"time"
)

// config json
// {
// "Input":{"Type":"b"},
// "Output":{"Type":"x"},
// "LogParser":{},
// "FilterOrder":"regexp,bayies",
// "FilterSettings":{"regexp":{},"bayies":{}},
// }

type LogProccessTask struct {
	Name           string
	InputSetting   map[string]string            `json:"Input"`
	Parser         *LogParser                   `json:"LogParser"`
	OutputSetting  map[string]string            `json:"Output"`
	FilterOrder    []string                     `json:"FilterOrder,omitempty"`
	FilterSettings map[string]map[string]string `json:"FilterSettings"`
	Filters        map[string]Filter
	StatsdAddr     string `json:"StatsdAddr"`
	statsd         *statsd.Statsd
	Input          DataSource
	Output         DataSink
	sync.Mutex
	configInfo []byte
	exitChan   chan int
}

type Filter interface {
	Handle(msg *map[string]interface{}) (*map[string]interface{}, error)
	Cleanup()
	SetStatsd(statsd *statsd.Statsd)
}

type DataSource interface {
	Stop()
	GetMsgChan() chan *[]byte
	SetStatsd(statsd *statsd.Statsd)
}

type DataSink interface {
	Stop()
	Start(msgChan chan *map[string]interface{})
	SetStatsd(statsd *statsd.Statsd)
}

func (t *LogProccessTask) Stop() {
	t.Input.Stop()
	t.Output.Stop()
	for _, f := range t.Filters {
		f.Cleanup()
	}
	close(t.exitChan)
}

func (t *LogProccessTask) GetName() string {
	return t.Name
}

func (t *LogProccessTask) DetailInfo() []byte {
	return t.configInfo
}

func NewLogProcessTask(name string, config []byte) (*LogProccessTask, error) {
	logProcessTask := &LogProccessTask{}
	if err := json.Unmarshal(config, logProcessTask); err != nil {
		log.Println("bad task config", err)
		return nil, fmt.Errorf("bad task config")
	}
	if logProcessTask.StatsdAddr == "" {
		logProcessTask.StatsdAddr = "127.0.0.1:8210"
	}
	logger := kitlog.NewJSONLogger(kitlog.NewSyncWriter(os.Stdout))
	logProcessTask.statsd = statsd.New("lazy.", logger)
	report := time.NewTicker(5 * time.Second)
	defer report.Stop()
	go logProcessTask.statsd.SendLoop(report.C, "udp", logProcessTask.StatsdAddr)
	logProcessTask.configInfo = config
	logProcessTask.Name = name
	logProcessTask.exitChan = make(chan int)
	logProcessTask.Filters = make(map[string]Filter)
	for k, v := range logProcessTask.FilterSettings {
		switch k {
		case "bayies":
			logProcessTask.Filters[k] = NewBayiesFilter(v)
		case "regexp":
			logProcessTask.Filters[k] = NewRegexpFilter(v)
		case "geoip2":
			logProcessTask.Filters[k] = NewGeoIP2Filter(v)
		}
	}
	var err error
	switch logProcessTask.InputSetting["Type"] {
	case "nsq":
		logProcessTask.Input, err = NewNSQReader(logProcessTask.InputSetting)
		if err != nil {
			return nil, err
		}
	//case "kafka":
	//	logParserTask.Input = NewKafkaReader(logParserTask.InputSetting)
	default:
		return nil, fmt.Errorf("not supported data source")
	}
	switch logProcessTask.OutputSetting["Type"] {
	case "elasticsearch":
		logProcessTask.Output, err = NewElasitcSearchWriter(logProcessTask.InputSetting)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("not supported sink")
	}
	return logProcessTask, nil
}

func (t *LogProccessTask) Start() error {
	msgChan := t.Input.GetMsgChan()
	parsedMsgChan := make(chan *map[string]interface{})
	t.Output.Start(parsedMsgChan)
	for {
		select {
		case msg := <-msgChan:
			rst, err := t.Parser.Handle(msg)
			if err != nil {
				log.Println(string(*msg), err)
				continue
			}
			for _, name := range t.FilterOrder {
				if f, ok := t.Filters[name]; ok {
					rst, err = f.Handle(rst)
					if err != nil && err.Error() != "ignore" {
						break
					}
				}
			}
			if err != nil {
				log.Println(string(*msg), err)
				continue
			}
			parsedMsgChan <- rst
		case <-t.exitChan:
			return nil
		}
	}
}
