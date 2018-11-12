package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"log"
	"strconv"
	"strings"
	"time"
)

// config
// {
// "KafkaBrokers":"127.0.0.1:9200,172.17.0.1:9200",
// "Topics":"xxx,xxx,xxx",
// "ConsumerGroup":"test",
// "User":"",
// "Password":"",
// "Type":"kafka"
// }

type KafkaReader struct {
	consumer *cluster.Consumer
	exitChan chan int
	msgChan  chan *map[string][]byte
}

func NewKafkaReader(config map[string]string) (*KafkaReader, error) {
	m := &KafkaReader{}
	m.msgChan = make(chan *map[string][]byte)
	m.exitChan = make(chan int)
	brokers := strings.Split(config["KafkaBrokers"], ",")
	topics := strings.Split(config["Topics"], ",")
	kafkaConfig := cluster.NewConfig()
	kafkaConfig.Consumer.Return.Errors = true
	kafkaConfig.Group.Return.Notifications = true
	if len(config["User"]) > 0 {
		kafkaConfig.Net.SASL.Enable = true
		kafkaConfig.Net.SASL.User = config["User"]
		kafkaConfig.Net.SASL.Password = config["Password"]
	}
	var err error
	m.consumer, err = cluster.NewConsumer(brokers, config["ConsumerGroup"], topics, kafkaConfig)
	go m.ReadLoop()
	log.Println("start consumer for topic", config["Topics"])
	return m, err
}

func (m *KafkaReader) ReadLoop() {
	// consume errors
	go func() {
		for err := range m.consumer.Errors() {
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	// consume notifications
	go func() {
		for ntf := range m.consumer.Notifications() {
			log.Printf("Rebalanced: %+v\n", ntf)
		}
	}()

	for {
		select {
		case msg, ok := <-m.consumer.Messages():
			if ok {
				logmsg := make(map[string][]byte)
				logmsg["msg"] = msg.Value
				m.msgChan <- &logmsg
				m.consumer.MarkOffset(msg, "")
			}
		case <-m.exitChan:
			m.consumer.Close()
			log.Println("exit kafka consumer")
			return
		}
	}
}

func (m *KafkaReader) Stop() {
	close(m.exitChan)
}
func (m *KafkaReader) GetMsgChan() chan *map[string][]byte {
	return m.msgChan
}

// config
// {
// "Topic":"xxxx",
// "KafkaBrokers":"127.0.0.1:9200,172.17.0.1:9200",
// "CompressionType":"snappy",
// "FlushFrequency":"",
// "User":"",
// "Password":"",
// "Type":"kafka"
// }

type KafkaWriter struct {
	producer sarama.AsyncProducer
	Topic    string
	exitChan chan int
}

func NewKafkaWriter(config map[string]string) (*KafkaWriter, error) {
	kafkaWriter := &KafkaWriter{}
	kafkaWriter.Topic = config["Topic"]
	kafkaWriter.exitChan = make(chan int)
	interval, err := strconv.Atoi(config["FlushFrequency"])
	if err != nil || interval < 500 {
		interval = 500
	}
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForLocal
	switch config["CompressionType"] {
	case "snappy":
		kafkaConfig.Producer.Compression = sarama.CompressionSnappy
	case "lz4":
		kafkaConfig.Producer.Compression = sarama.CompressionLZ4
	case "gzip":
		kafkaConfig.Producer.Compression = sarama.CompressionGZIP
	case "zstd":
		kafkaConfig.Producer.Compression = sarama.CompressionZSTD
	default:
		kafkaConfig.Producer.Compression = sarama.CompressionNone
	}
	kafkaConfig.Producer.Flush.Frequency = time.Duration(int64(interval)) * time.Millisecond

	if len(config["User"]) > 0 {
		kafkaConfig.Net.SASL.Enable = true
		kafkaConfig.Net.SASL.User = config["User"]
		kafkaConfig.Net.SASL.Password = config["Password"]
	}
	kafkaWriter.producer, err = sarama.NewAsyncProducer(strings.Split(config["KafkaBrokers"], ","), kafkaConfig)
	return kafkaWriter, err
}

func (kafkaWriter *KafkaWriter) Stop() {
	close(kafkaWriter.exitChan)
}

func (kafkaWriter *KafkaWriter) Start(dataChan chan *map[string]interface{}) {
	for {
		select {
		case <-kafkaWriter.exitChan:
			kafkaWriter.producer.Close()
			log.Println("exit kafka producer")
			return
		case logmsg := <-dataChan:
			kafkaWriter.producer.Input() <- &sarama.ProducerMessage{Topic: kafkaWriter.Topic, Key: nil, Value: sarama.StringEncoder((*logmsg)["rawmsg"].(string))}
		case err := <-kafkaWriter.producer.Errors():
			if err != nil {
				fmt.Println(err.Err)
			}
		}
	}
}
