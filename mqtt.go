package main

import (
	"fmt"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/prometheus/client_golang/prometheus"
)

// config
// {
// "BrokerURL":"tcp://localhost:1883",
// "Name":"mqttreader",
// "Topic":"#",
// "UserName":"xxx",
// "Password":"xxxx",
// "CleanSession":"true",
// "Type":"mqtt"
// }

type MQTTReader struct {
	client       mqtt.Client
	Topic        string
	msgChan      chan *map[string][]byte
	metricstatus *prometheus.CounterVec
}

func NewMQTTReader(config map[string]string) (*MQTTReader, error) {
	m := &MQTTReader{}
	m.msgChan = make(chan *map[string][]byte)
	m.Topic = config["Topic"]
	opts := mqtt.NewClientOptions()
	opts.AddBroker(config["BrokerURL"])
	opts.SetClientID(config["Name"])
	opts.SetUsername(config["UserName"])
	opts.SetPassword(config["Password"])
	if config["CleanSession"] == "true" {
		opts.SetCleanSession(true)
	}
	opts.SetDefaultPublishHandler(m.HandleDate)
	opts.SetOnConnectHandler(m.onConnect)
	opts.SetConnectionLostHandler(m.onLost)
	m.client = mqtt.NewClient(opts)
	if token := m.client.Connect(); token.Wait() && token.Error() != nil {
		return m, token.Error()
	}
	fmt.Println(config["Name"], "mqtt reader is started")
	m.metricstatus = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "lazy_intput",
			Name:      "mqtt_consumer",
			Help:      "mqtt reader status.",
		},
		[]string{"count"},
	)
	// Register status
	prometheus.Register(m.metricstatus)
	return m, nil
}
func (m *MQTTReader) HandleDate(client mqtt.Client, msg mqtt.Message) {
	payload := msg.Payload()
	if string(payload) == "Connected" {
		return
	}
	topic := msg.Topic()
	logmsg := make(map[string][]byte)
	logmsg["msg"] = []byte(fmt.Sprintf("%s %s", topic, payload))
	m.msgChan <- &logmsg
	m.metricstatus.WithLabelValues("message").Inc()
}
func (m *MQTTReader) onConnect(client mqtt.Client) {
	if token := m.client.Subscribe(m.Topic, byte(0), nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
	}
}
func (m *MQTTReader) Stop() {
	m.client.Disconnect(1)
	prometheus.Unregister(m.metricstatus)
}
func (m *MQTTReader) onLost(client mqtt.Client, err error) {
	fmt.Println(err, m.Topic)
}
func (m *MQTTReader) GetMsgChan() chan *map[string][]byte {
	return m.msgChan
}
