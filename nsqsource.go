package main

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/nsqio/go-nsq"
	"github.com/olivere/elastic"
	"log"
	"os"
	"strings"
	"time"
)

type NSQReadTask struct {
	consumer   *nsq.Consumer
	logsetting *LogSetting
	msgChan    chan *elastic.BulkIndexRequest
}

func NewNSQTask(logsetting *LogSetting) chan *elastic.BulkIndexRequest {
	m := &NSQReadTask{}
	m.msgChan = make(chan *elastic.BulkIndexRequest)
	m.logsetting = logsetting
	m.logsetting.SetRules()
	cfg := nsq.NewConfig()
	hostname, err := os.Hostname()
	if err != nil {
		log.Println(err)
	}
	cfg.Set("user_agent", fmt.Sprintf("lazy/%s", hostname))
	cfg.Set("snappy", true)
	cfg.Set("max_in_flight", m.logsetting.TasksCount)
	m.consumer, err = nsq.NewConsumer(m.logsetting.Config["Topic"], m.logsetting.Config["Channel"], cfg)
	m.consumer.AddConcurrentHandlers(m, m.logsetting.TasksCount)
	lookupds := strings.Split(m.logsetting.Config["LookupdAddresses"], ",")
	err = m.consumer.ConnectToNSQLookupds(lookupds)
	return m.msgChan
}

func (m *NSQReadTask) HandleMessage(msg *nsq.Message) error {
	var logFormat LogFormat
	var logmsg []byte
	if m.logsetting.LogType == "rfc3164" {
		err := proto.Unmarshal(msg.Body, &logFormat)
		if err != nil {
			log.Println("proto unmarshal", err, string(msg.Body))
			return nil
		}
		logmsg = []byte(logFormat.GetRawmsg())
		if len(logmsg) < 1 {
			return nil
		}
	} else {
		logmsg = msg.Body
	}
	parsedLog, err := m.logsetting.Parser(logmsg)
	if err != nil {
		log.Println(err, logmsg, logFormat.GetFrom())
		return nil
	}
	if m.logsetting.LogType == "rfc3164" {
		(*parsedLog)["from"] = logFormat.GetFrom()
	} else {
		if _, ok := (*parsedLog)["timestamp"]; !ok {
			(*parsedLog)["timestamp"] = time.Now()
		}
	}
	if value, ok := (*parsedLog)[m.logsetting.DispatchKey]; ok {
		tag := m.logsetting.Proccessors[value.(string)].Tag
		m.logsetting.Proccessors[value.(string)].Handler(parsedLog)
		if (*parsedLog)[fmt.Sprintf("%s_RegexpCheck", tag)] == "ignore" {
			return nil
		}
		if (*parsedLog)[fmt.Sprintf("%s_RegexpCheck", tag)] == "citical" {
			fmt.Println(parsedLog)
		}
	}
	if _, ok := m.logsetting.Proccessors["default"]; ok {
		m.logsetting.Proccessors["default"].Handler(parsedLog)
	}
	m.msgChan <- elastic.NewBulkIndexRequest().Doc(parsedLog).Type(m.logsetting.LogType)
	return nil
}

func (m *NSQReadTask) Stop() {
	m.consumer.Stop()
}
