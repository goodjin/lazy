package main

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/consul/api"
	"gopkg.in/olivere/elastic.v3"
	"log"
	"sync"
	"time"
)

type LogParserPool struct {
	sync.Mutex
	*Setting
	checklist     map[string]string
	exitChannel   chan int
	logParserList map[string]*LogParser
	client        *api.Client
}

func (m *LogParserPool) Stop() {
	close(m.exitChannel)
	m.Lock()
	defer m.Unlock()
	for k := range m.logParserList {
		m.logParserList[k].Stop()
	}
}

func (m *LogParserPool) Run() error {
	config := api.DefaultConfig()
	config.Address = m.ConsulAddress
	config.Datacenter = m.Datacenter
	config.Token = m.Token
	var err error
	m.client, err = api.NewClient(config)
	if err != nil {
		return err
	}
	err = m.getLogTopics()
	go m.syncLogTopics()
	return err
}
func (m *LogParserPool) syncLogTopics() {
	ticker := time.Tick(time.Second * 60)
	for {
		select {
		case <-ticker:
			err := m.getLogTopics()
			if err != nil {
				log.Println(err)
			}
		case <-m.exitChannel:
			return
		}
	}
}

//"%s/topics"
func (m *LogParserPool) getLogTopics() error {
	topicsKey := fmt.Sprintf("%s/topics", m.ConsulKey)
	conf, err := m.ReadConfigFromConsul(topicsKey)
	if err != nil {
		return err
	}
	m.Lock()
	for k, v := range conf {
		if m.checklist[k] == v {
			continue
		}
		var logSetting LogSetting
		err = json.Unmarshal([]byte(v), &logSetting)
		if err != nil {
			return err
		}
		if _, ok := m.logParserList[k]; !ok {
			w := &LogParser{
				Setting:     m.Setting,
				logTopic:    k,
				regexMap:    make(map[string][]*RegexpSetting),
				exitChannel: make(chan int),
				msgChannel:  make(chan *elastic.BulkIndexRequest),
				logSetting:  &logSetting,
			}
			if err := w.Run(); err != nil {
				log.Println(k, err)
				continue
			}
			m.logParserList[k] = w
		} else {
			if m.logParserList[k] != nil {
				m.logParserList[k].Stop()
			}
			w := &LogParser{
				Setting:     m.Setting,
				logTopic:    k,
				regexMap:    make(map[string][]*RegexpSetting),
				exitChannel: make(chan int),
				msgChannel:  make(chan *elastic.BulkIndexRequest),
				logSetting:  &logSetting,
			}
			if err := w.Run(); err != nil {
				log.Println(k, err)
				continue
			}
			m.logParserList[k] = w
		}
		m.checklist[k] = v
	}
	m.Unlock()
	return nil
}

func (m *LogParserPool) ReadConfigFromConsul(key string) (map[string]string, error) {
	consulSetting := make(map[string]string)
	kv := m.client.KV()
	pairs, _, err := kv.List(key, nil)
	if err != nil {
		return consulSetting, err
	}
	size := len(key) + 1
	for _, value := range pairs {
		if len(value.Key) > size {
			consulSetting[value.Key[size:]] = string(value.Value)
		}
	}
	return consulSetting, err
}
