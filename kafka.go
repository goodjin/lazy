package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/prometheus/client_golang/prometheus"
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
	consumer     *cluster.Consumer
	exitChan     chan int
	msgChan      chan *map[string][]byte
	metricstatus *prometheus.CounterVec
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
	if len(config["KafkaVersion"]) > 0 {
		kafkaConfig.Version, err = sarama.ParseKafkaVersion(config["KafkaVersion"])
		if err != nil {
			return m, err
		}
	}
	m.consumer, err = cluster.NewConsumer(brokers, config["ConsumerGroup"], topics, kafkaConfig)
	if err != nil {
		return m, err
	}
	go m.ReadLoop()
	log.Println("start consumer for topic", config["Topics"])
	m.metricstatus = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "lazy_input",
			Name:      fmt.Sprintf("kafka_consumer_%s", strings.Replace(config["Name"], "-", "_", -1)),
			Help:      "kafka consumer status.",
		},
		[]string{"method"},
	)
	// Register status
	prometheus.Register(m.metricstatus)
	return m, err
}

func (m *KafkaReader) ReadLoop() {
	// consume errors
	go func() {
		for err := range m.consumer.Errors() {
			m.metricstatus.WithLabelValues("err_count").Inc()
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	// consume notifications
	go func() {
		for ntf := range m.consumer.Notifications() {
			m.metricstatus.WithLabelValues("notification").Inc()
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
				m.metricstatus.WithLabelValues("message_count").Inc()
			}
		case <-m.exitChan:
			m.consumer.Close()
			log.Println("exit kafka consumer")
			return
		}
	}
}

func (m *KafkaReader) Stop() {
	prometheus.Unregister(m.metricstatus)
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
	producer     sarama.AsyncProducer
	Topic        string
	exitChan     chan int
	metricstatus *prometheus.CounterVec
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
	if len(config["KafkaVersion"]) > 0 {
		kafkaConfig.Version, err = sarama.ParseKafkaVersion(config["KafkaVersion"])
		if err != nil {
			return kafkaWriter, err
		}
	}
	kafkaWriter.producer, err = sarama.NewAsyncProducer(strings.Split(config["KafkaBrokers"], ","), kafkaConfig)
	if err != nil {
		return kafkaWriter, err
	}
	kafkaWriter.metricstatus = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "lazy_output",
			Name:      fmt.Sprintf("kafka_producer_%s", strings.Replace(config["Topic"], "-", "_", -1)),
			Help:      "kafka producer status.",
		},
		[]string{"method"},
	)
	// Register status
	prometheus.Register(kafkaWriter.metricstatus)
	return kafkaWriter, err
}

func (kafkaWriter *KafkaWriter) Stop() {
	close(kafkaWriter.exitChan)
	prometheus.Unregister(kafkaWriter.metricstatus)
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
			kafkaWriter.metricstatus.WithLabelValues("message_count").Inc()
		case err := <-kafkaWriter.producer.Errors():
			if err != nil {
				fmt.Println(err.Err)
			}
		}
	}
}
