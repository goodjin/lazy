package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"strings"
)

// config
// {
// "FileName":"./xxx",
// "ReadAll":"true",
// "Type":"file"
// }

type KafkaReader struct {
	consumer          sarama.Consumer
	partitionConsumer []sarama.PartitionConsumer
	exitChan          chan int
	msgChan           chan *map[string][]byte
}

func NewKafkaReader(config map[string]string) (*KafkaReader, error) {
	m := &KafkaReader{}
	m.msgChan = make(chan *map[string][]byte)
	m.exitChan = make(chan int)
	addrs := strings.Split(config["KafkaAddresses"], ",")
	var err error
	m.consumer, err = sarama.NewConsumer(addrs, nil)
	if err != nil {
		return m, err
	}
	partitions, err := m.consumer.Partitions(config["Topic"])
	for index, patition := range partitions {
		m.partitionConsumer[index], err = m.consumer.ConsumePartition(config["Topic"], patition, sarama.OffsetNewest)
		partitionConsumer := m.partitionConsumer[index]
		go func() {
			logmsg := make(map[string][]byte)
			for {
				select {
				case msg := <-partitionConsumer.Messages():
					logmsg["msg"] = msg.Value
					m.msgChan <- &logmsg
				case <-m.exitChan:
					return
				}
			}
		}()
	}
	return m, err
}

func (m *KafkaReader) Stop() {
	close(m.exitChan)
	for _, v := range m.partitionConsumer {
		v.Close()
	}
	m.consumer.Close()
}
func (m *KafkaReader) GetMsgChan() chan *map[string][]byte {
	return m.msgChan
}
