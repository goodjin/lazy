package main

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/hashicorp/consul/api"
	"github.com/prometheus/client_golang/prometheus"
)

// LazyConfig lazy config info
type LazyConfig struct {
	ConsulAddress string `json:"ConsulAddress"`
	Datacenter    string `json:"Datacenter"`
	Token         string `json:"ConsulToken"`
	ConsulKey     string `json:"ConsulKey"`
	MetricAddr    string `json:"MetricAddr"`
	client        *api.Client
}

// ReadConfig used to read json to config
func ReadConfig(file string) (*LazyConfig, error) {
	configFile, err := os.Open(file)
	config, err := ioutil.ReadAll(configFile)
	if err != nil {
		return nil, err
	}
	configFile.Close()
	setting := &LazyConfig{}
	if err := json.Unmarshal(config, setting); err != nil {
		return nil, err
	}
	if setting.MetricAddr == "" {
		setting.MetricAddr = "0.0.0.0:7080"
	}
	prometheus.MustRegister(prometheus.NewBuildInfoCollector())
	return setting, err
}

// InitConfig init config
func (m *LazyConfig) InitConfig() error {
	config := api.DefaultConfig()
	config.Address = m.ConsulAddress
	config.Datacenter = m.Datacenter
	config.Token = m.Token
	var err error
	m.client, err = api.NewClient(config)
	return err
}

// ReadConfigFromConsul read config from consul
func (m *LazyConfig) ReadConfigFromConsul(key string) (map[string]string, error) {
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
