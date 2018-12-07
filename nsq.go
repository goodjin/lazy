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

// {
// "Topic":"xxxx",
// "Name":"task",
// "NSQAddress":"127.0.0.1:9200,172.17.0.1:9200",
// "CompressionType":"snappy",
// "BatchSize":"20"
// }

type NSQWriter struct {
	producer  *nsq.Producer
	Topic     string
	BatchSize int
	exitChan  chan int
}

func NewNSQWriter(config map[string]string) (*NSQWriter, error) {
	nsqWriter := &NSQWriter{}
	nsqWriter.Topic = config["Topic"]
	nsqWriter.BatchSize, _ = strconv.Atoi(config["BatchSize"])
	cfg := nsq.NewConfig()
	hostname, err := os.Hostname()
	cfg.Set("user_agent", fmt.Sprintf("%s/%s", config["Name"], hostname))
	if config["CompressionType"] != "" {
		cfg.Set(config["CompressionType"], true)
	}
	nsqWriter.exitChan = make(chan int)
	nsqWriter.producer, err = nsq.NewProducer(config["NSQAddress"], cfg)
	return nsqWriter, err
}

func (nsqWriter *NSQWriter) Stop() {
	nsqWriter.producer.Stop()
	close(nsqWriter.exitChan)
	log.Println("exit nsq producer")
}

func (nsqWriter *NSQWriter) Start(dataChan chan *map[string]interface{}) {
	var body [][]byte
	for {
		select {
		case <-nsqWriter.exitChan:
			return
		case logmsg := <-dataChan:
			item := (*logmsg)["rawmsg"].(string)
			if nsqWriter.BatchSize > 1 {
				if len(body) < nsqWriter.BatchSize {
					body = append(body, []byte(item))
					break
				}
				nsqWriter.producer.MultiPublish(nsqWriter.Topic, body)
				body = body[:0]
			} else {
				nsqWriter.producer.Publish(nsqWriter.Topic, []byte(item))
			}
		}
	}
}
