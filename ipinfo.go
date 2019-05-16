package main

import (
	"encoding/json"
	"fmt"
	"github.com/zmap/go-iptree/iptree"
	"strings"
	"io/ioutil"
	"os"
)

// config json
// {
// "KeyToFilter":"syslogtag",
// "File":"./info.json"
// }

type IPinfoFilter struct {
	KeyToFilter string `json:"KeyToFilter"`
	IPTree      *iptree.IPTree
}

func NewIPinfoFilter(config map[string]string) *IPinfoFilter {
	rf := &IPinfoFilter{
		KeyToFilter: config["KeyToFilter"],
	}
	rf.IPTree = iptree.New()
	var err error
	configFile, err := os.Open(config["File"])
	if err != nil {
		return rf
	}
	body, err := ioutil.ReadAll(configFile)
	if err != nil {
		return rf
	}
	configFile.Close()
	var settings map[string]string
	if err := json.Unmarshal(body, &settings); err != nil {
		return rf
	}
	for k, v := range settings {
		rf.IPTree.AddByString(k, v)
	}
	return rf
}

func (rf *IPinfoFilter) Cleanup() {
}

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
