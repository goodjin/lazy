package main

import (
	"fmt"
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
	msgChan   chan *map[string][]byte
}

func NewNSQReader(config map[string]string) (*NSQReader, error) {
	m := &NSQReader{}
	m.msgChan = make(chan *map[string][]byte)
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
	fmt.Println(config["Name"], "nsq reader is started")
	return m, err
}

func (m *NSQReader) HandleMessage(msg *nsq.Message) error {
	var logFormat LogFormat
	logmsg := make(map[string][]byte)
	switch m.msgFormat {
	case "protobuf":
		err := proto.Unmarshal(msg.Body, &logFormat)
		if err != nil {
			log.Println("proto unmarshal", err, string(msg.Body))
			return nil
		}
		logmsg["msg"] = []byte(logFormat.GetRawmsg())
		logmsg["from"] = []byte(logFormat.GetFrom())
		if len(logmsg) < 1 {
			return nil
		}
	default:
		logmsg["msg"] = msg.Body
	}
	m.msgChan <- &logmsg
	return nil
}

func (m *NSQReader) Stop() {
	m.consumer.Stop()
}
func (m *NSQReader) GetMsgChan() chan *map[string][]byte {
	return m.msgChan
}
