package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
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
	Input          DataSource
	Output         DataSink
	sync.Mutex
	configInfo []byte
	exitChan   chan int
}

type Filter interface {
	Handle(msg *map[string]interface{}) (*map[string]interface{}, error)
	Cleanup()
}

type DataSource interface {
	Stop()
	GetMsgChan() chan *[]byte
}

type DataSink interface {
	Stop()
	Start(msgChan chan *map[string]interface{})
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

func NewLogProccessTask(name string, config []byte) (*LogProccessTask, error) {
	logParserTask := &LogProccessTask{}
	if err := json.Unmarshal(config, logParserTask); err != nil {
		log.Println("bad task config", err)
		return nil, fmt.Errorf("bad task config")
	}
	logParserTask.configInfo = config
	logParserTask.Name = name
	logParserTask.exitChan = make(chan int)
	logParserTask.Filters = make(map[string]Filter)
	for k, v := range logParserTask.FilterSettings {
		switch k {
		case "bayies":
			logParserTask.Filters[k] = NewBayiesFilter(v)
		case "regexp":
			logParserTask.Filters[k] = NewRegexpFilter(v)
		case "geoip2":
			logParserTask.Filters[k] = NewGeoIP2Filter(v)
		}
	}
	var err error
	switch logParserTask.InputSetting["Type"] {
	case "nsq":
		logParserTask.Input, err = NewNSQReader(logParserTask.InputSetting)
		if err != nil {
			return nil, err
		}
	//case "kafka":
	//	logParserTask.Input = NewKafkaReader(logParserTask.InputSetting)
	default:
		return nil, fmt.Errorf("not supported data source")
	}
	switch logParserTask.OutputSetting["Type"] {
	case "elasticsearch":
		logParserTask.Output, err = NewElasitcSearchWriter(logParserTask.InputSetting)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("not supported sink")
	}
	return logParserTask, nil
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
