package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

// Config is metrictools config struct
type Setting struct {
	NsqdAddress      string   `json:"nsqdAddr"`
	LookupdAddresses []string `json:"lookupdAddresses"`
	TrainTopic       string   `json:"trainTopic"`
	MaxInFlight      int      `json:"maxinflight"`
	ConsulAddress    string   `json:"consulAddress"`
	Datacenter       string   `json:"datacenter"`
	Token            string   `json:"consulToken"`
	ConsulKey        string   `json:"consulKey"`
	LogChannel       string   `json:"logChannel"`
}

// ReadConfig used to read json to config
func ReadConfig(file string) (*Setting, error) {
	configFile, err := os.Open(file)
	config, err := ioutil.ReadAll(configFile)
	if err != nil {
		return nil, err
	}
	configFile.Close()
	setting := &Setting{}
	if err := json.Unmarshal(config, setting); err != nil {
		return nil, err
	}
	return setting, err
}
