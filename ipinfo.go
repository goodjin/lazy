package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/zmap/go-iptree/iptree"
)

// config json
// {
// "KeyToFilter":"syslogtag",
// "File":"./info.json"
// }

// IPinfoFilter for ip address info
type IPinfoFilter struct {
	KeyToFilter string `json:"KeyToFilter"`
	IPTree      *iptree.IPTree
}

// NewIPinfoFilter create IPinfoFilter
func NewIPinfoFilter(config map[string]string) (*IPinfoFilter, error) {
	rf := &IPinfoFilter{
		KeyToFilter: config["KeyToFilter"],
	}
	rf.IPTree = iptree.New()
	var err error
	configFile, err := os.Open(config["File"])
	if err != nil {
		return rf, err
	}
	body, err := ioutil.ReadAll(configFile)
	if err != nil {
		return rf, err
	}
	configFile.Close()
	var settings map[string]string
	if err := json.Unmarshal(body, &settings); err != nil {
		return rf, err
	}
	for k, v := range settings {
		rf.IPTree.AddByString(k, v)
	}
	return rf, nil
}

// Cleanup remove all
func (rf *IPinfoFilter) Cleanup() {
}

// Handle msg
func (rf *IPinfoFilter) Handle(msg *map[string]interface{}) (*map[string]interface{}, error) {
	info, ok := (*msg)[rf.KeyToFilter].(string)
	if !ok {
		return msg, fmt.Errorf("bad data format, not a string")
	}
	ipaddr := strings.Split(info, ":")[0]
	v, ok, _ := rf.IPTree.GetByString(ipaddr)
	if ok {
		(*msg)["BackendEnv"] = v
	}
	return msg, nil
}
