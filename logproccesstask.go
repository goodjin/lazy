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
	Name          string
	InputSetting  map[string]string `json:"Input"`
	Parser        *LogParser        `json:"LogParser"`
	OutputSetting map[string]string `json:"Output"`
	// filter settting
	FilterOrder    []string                     `json:"FilterOrder,omitempty"`
	FilterSettings map[string]map[string]string `json:"FilterSettings"`
	Filters        map[string]Filter

	Input  DataSource
	Output DataSink
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
	GetMsgChan() chan *map[string][]byte
}

type DataSink interface {
	Stop()
	Start(msgChan chan *map[string]interface{})
}

func (t *LogProccessTask) Stop() {
	close(t.exitChan)
	t.Input.Stop()
	for _, f := range t.Filters {
		f.Cleanup()
	}
	t.Output.Stop()
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
	logProcessTask.configInfo = config
	logProcessTask.Name = name
	logProcessTask.exitChan = make(chan int)
	logProcessTask.Filters = make(map[string]Filter)
	for k, v := range logProcessTask.FilterSettings {
		switch v["Type"] {
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
	case "file":
		logProcessTask.Input, err = NewFileReader(logProcessTask.InputSetting)
		if err != nil {
			return nil, err
		}
	case "kafka":
		logProcessTask.Input, err = NewKafkaReader(logProcessTask.InputSetting)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("not supported data source")
	}
	switch logProcessTask.OutputSetting["Type"] {
	case "elasticsearch":
		logProcessTask.Output, err = NewElasitcSearchWriter(logProcessTask.OutputSetting)
		if err != nil {
			return nil, err
		}
	case "kafka":
		logProcessTask.Output, err = NewKafkaWriter(logProcessTask.OutputSetting)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("not supported sink")
	}
	return logProcessTask, nil
}

func (t *LogProccessTask) Run() {
	msgChan := t.Input.GetMsgChan()
	parsedMsgChan := make(chan *map[string]interface{})
	go t.Output.Start(parsedMsgChan)
	for {
		select {
		case msg := <-msgChan:
			rst, err := t.Parser.Handle(msg)
			if err != nil {
				log.Println(string((*msg)["msg"]), err)
				break
			}
			for _, name := range t.FilterOrder {
				if f, ok := t.Filters[name]; ok {
					rst, err = f.Handle(rst)
					if err != nil && err.Error() == "ignore" {
						break
					}
				}
			}
			parsedMsgChan <- rst
		case <-t.exitChan:
			return
		}
	}
}

func (t *LogProccessTask) IsGoodConfig(config []byte) bool {
	logProcessTask := &LogProccessTask{}
	if err := json.Unmarshal(config, logProcessTask); err != nil {
		return false
	}
	return true
}
