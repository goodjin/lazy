package main

import (
	"encoding/json"
	"github.com/hashicorp/consul/api"
	"io/ioutil"
	"os"
)

type LogTaskConfig struct {
	ConsulAddress string `json:"ConsulAddress"`
	Datacenter    string `json:"Datacenter"`
	Token         string `json:"ConsulToken"`
	ConsulKey     string `json:"ConsulKey"`
	client        *api.Client
}

// ReadConfig used to read json to config
func ReadConfig(file string) (*LogTaskConfig, error) {
	configFile, err := os.Open(file)
	config, err := ioutil.ReadAll(configFile)
	if err != nil {
		return nil, err
	}
	configFile.Close()
	setting := &LogTaskConfig{}
	if err := json.Unmarshal(config, setting); err != nil {
		return nil, err
	}
	return setting, err
}

func (m *LogTaskConfig) InitConfig() error {
	config := api.DefaultConfig()
	config.Address = m.ConsulAddress
	config.Datacenter = m.Datacenter
	config.Token = m.Token
	var err error
	m.client, err = api.NewClient(config)
	return err
}

func (m *LogTaskConfig) ReadConfigFromConsul(key string) (map[string]string, error) {
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
