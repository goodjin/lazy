package main

import (
	"fmt"
	"github.com/go-kit/kit/metrics/statsd"
	"github.com/golang/protobuf/proto"
	"github.com/nsqio/go-nsq"
	"log"
	"os"
	"strconv"
	"strings"
)

// config
// {
// "MessageFormat":"protobuf",
// "Name":"syslogreader",
// "MaxInFlight":"10",
// "Topic":"syslog",
// "Channel":"aasa",
// "LookupdAddresses":"127.0.0.1:4150,127.0.0.2:4151"
// "Type":"elasticsearch"
// }

type NSQReader struct {
	consumer  *nsq.Consumer
	msgFormat string
	msgChan   chan *[]byte
	statsd    *statsd.Statsd
}

func NewNSQReader(config map[string]string) (*NSQReader, error) {
	m := &NSQReader{}
	m.msgChan = make(chan *[]byte)
	m.msgFormat = config["MessageFormat"]
	cfg := nsq.NewConfig()
	hostname, err := os.Hostname()
	if err != nil {
		log.Println(err)
	}
	cfg.Set("user_agent", fmt.Sprintf("%s/%s", config["Name"], hostname))
	cfg.Set("snappy", true)
	taskscount, err := strconv.Atoi(config["MaxInFlight"])
	if err != nil {
		taskscount = 100
	}
	cfg.Set("max_in_flight", taskscount)
	m.consumer, err = nsq.NewConsumer(config["Topic"], config["Channel"], cfg)
	m.consumer.AddConcurrentHandlers(m, taskscount)
	lookupds := strings.Split(config["LookupdAddresses"], ",")
	err = m.consumer.ConnectToNSQLookupds(lookupds)
	return m, err
}

func (m *NSQReader) HandleMessage(msg *nsq.Message) error {
	var logFormat LogFormat
	var logmsg []byte
	switch m.msgFormat {
	case "protobuf":
		err := proto.Unmarshal(msg.Body, &logFormat)
		if err != nil {
			log.Println("proto unmarshal", err, string(msg.Body))
			return nil
		}
		logmsg = []byte(logFormat.GetRawmsg())
		if len(logmsg) < 1 {
			return nil
		}
	default:
		logmsg = msg.Body
	}
	m.msgChan <- &logmsg
	return nil
}

func (m *NSQReader) Stop() {
	m.consumer.Stop()
}
func (m *NSQReader) GetMsgChan() chan *[]byte {
	return m.msgChan
}
func (m *NSQReader) SetStatsd(statsd *statsd.Statsd) {
	m.statsd = statsd
}
