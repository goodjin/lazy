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
	Name          string
	InputSetting  map[string]string `json:"Input"`
	Parser        *LogParser        `json:"LogParser"`
	OutputSetting map[string]string `json:"Output"`
	// filter settting
	FilterOrder    []string                     `json:"FilterOrder,omitempty"`
	FilterSettings map[string]map[string]string `json:"FilterSettings"`
	Filters        map[string]Filter
	// statsd
	StatsdAddr string `json:"StatsdAddr"`
	statsd     *statsd.Statsd

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
	logProcessTask.Parser.SetStatsd(logProcessTask.statsd)
	logProcessTask.configInfo = config
	logProcessTask.Name = name
	logProcessTask.exitChan = make(chan int)
	logProcessTask.Filters = make(map[string]Filter)
	for k, v := range logProcessTask.FilterSettings {
		switch v["Type"] {
		case "bayies":
			logProcessTask.Filters[k] = NewBayiesFilter(v, logProcessTask.statsd)
		case "regexp":
			logProcessTask.Filters[k] = NewRegexpFilter(v, logProcessTask.statsd)
		case "geoip2":
			logProcessTask.Filters[k] = NewGeoIP2Filter(v, logProcessTask.statsd)
		}
	}
	var err error
	switch logProcessTask.InputSetting["Type"] {
	case "nsq":
		logProcessTask.Input, err = NewNSQReader(logProcessTask.InputSetting, logProcessTask.statsd)
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
		logProcessTask.Output, err = NewElasitcSearchWriter(logProcessTask.InputSetting, logProcessTask.statsd)
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

func (t *LogProccessTask) IsGoodConfig(config []byte) bool {
	logProcessTask := &LogProccessTask{}
	if err := json.Unmarshal(config, logProcessTask); err != nil {
		return false
	}
	return true
}
