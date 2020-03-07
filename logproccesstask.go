package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
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

// LogProccessTask log procceser
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

// Filter msg filter
type Filter interface {
	Handle(msg *map[string]interface{}) (*map[string]interface{}, error)
	Cleanup()
}

// DataSource msg source
type DataSource interface {
	Stop()
	GetMsgChan() chan *map[string][]byte
}

// DataSink msg dest
type DataSink interface {
	Stop()
	Start(msgChan chan *map[string]interface{})
}

// Stop stop proccess task
func (t *LogProccessTask) Stop() {
	close(t.exitChan)
	t.Input.Stop()
	for _, f := range t.Filters {
		f.Cleanup()
	}
	t.Output.Stop()
}

// GetName get task name
func (t *LogProccessTask) GetName() string {
	return t.Name
}

// DetailInfo return config
func (t *LogProccessTask) DetailInfo() []byte {
	return t.configInfo
}

// NewLogProcessTask create LogProcessTask
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
	taskname := strings.ReplaceAll(name, "-", "_")
	for k, v := range logProcessTask.FilterSettings {
		v["Taskname"] = taskname
		switch v["Type"] {
		case "bayies":
			bayiesFilter, err := NewBayiesFilter(v)
			if err == nil {
				logProcessTask.Filters[k] = bayiesFilter
			} else {
				log.Printf("init %s error: %s", v["Type"], err)
			}
		case "regexp":
			regexpFilter, err := NewRegexpFilter(v)
			if err == nil {
				logProcessTask.Filters[k] = regexpFilter
			} else {
				log.Printf("init %s error: %s", v["Type"], err)
			}
		case "geoip2":
			geoIP2Filter, err := NewGeoIP2Filter(v)
			if err == nil {
				logProcessTask.Filters[k] = geoIP2Filter
			} else {
				log.Printf("init %s error: %s", v["Type"], err)
			}
		case "lstm":
			lstmFilter, err := NewLSTMFilter(v)
			if err == nil {
				logProcessTask.Filters[k] = lstmFilter
			} else {
				log.Printf("init %s error: %s", v["Type"], err)
			}
		case "ipinfo":
			ipinfoFilter, err := NewIPinfoFilter(v)
			if err == nil {
				logProcessTask.Filters[k] = ipinfoFilter
			} else {
				log.Printf("init %s error: %s", v["Type"], err)
			}
		case "sample":
			sampleFilter, err := NewSampleFilter(v)
			if err == nil {
				logProcessTask.Filters[k] = sampleFilter
			} else {
				log.Printf("init %s error: %s", v["Type"], err)
			}
		}
	}
	logProcessTask.InputSetting["Taskname"] = taskname
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
	case "mqtt":
		logProcessTask.Input, err = NewMQTTReader(logProcessTask.InputSetting)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("not supported data source")
	}
	logProcessTask.OutputSetting["Taskname"] = taskname
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
	case "nsq":
		logProcessTask.Output, err = NewNSQWriter(logProcessTask.OutputSetting)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("not supported sink")
	}
	return logProcessTask, nil
}

// Run start task
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
			if err != nil && err.Error() == "ignore" {
				break
			}
			parsedMsgChan <- rst
		case <-t.exitChan:
			return
		}
	}
}

// IsGoodConfig check task config
func (t *LogProccessTask) IsGoodConfig(config []byte) bool {
	logProcessTask := &LogProccessTask{}
	if err := json.Unmarshal(config, logProcessTask); err != nil {
		return false
	}
	return true
}
