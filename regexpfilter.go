package main

import (
	"fmt"
	"regexp"
)

// config json
// {
// "KeyToFilter":"syslogtag",
// "HashKey":"tag",
// "b":"good,bad",
// "ignore":"a,b,c",
// }

// RegexpFilter regexp filter
type RegexpFilter struct {
	HashKey       string            `json:"HashKey"`
	KeyToFilter   string            `json:"KeyToFilter"`
	LabelName     string            `json:"LabelName"`
	RegexpSetting map[string]string `json:"RegexpSetting,omitempty"`
	regexpList    map[string]*regexp.Regexp
}

// NewRegexpFilter create RegexpFilter
func NewRegexpFilter(config map[string]string) (*RegexpFilter, error) {
	rf := &RegexpFilter{
		KeyToFilter: config["KeyToFilter"],
	}
	rf.RegexpSetting = make(map[string]string)
	rf.HashKey = config["HashKey"]
	rf.LabelName = config["LabelName"]
	delete(config, "KeyToFilter")
	delete(config, "HashKey")
	delete(config, "Type")
	delete(config, "LabelName")
	rf.regexpList = make(map[string]*regexp.Regexp)
	var err error
	for k, v := range config {
		rf.regexpList[k], err = regexp.CompilePOSIX(v)
		if err != nil {
			delete(rf.regexpList, k)
			fmt.Println(k, v, err)
		}
	}
	if len(rf.regexpList) == 0 {
		return rf, fmt.Errorf("null regexp")
	}
	return rf, nil
}

// Handle filter messages
func (rf *RegexpFilter) Handle(msg *map[string]interface{}) (*map[string]interface{}, error) {
	message := (*msg)[rf.KeyToFilter]
	var hashkey string
	if value, ok := (*msg)[rf.HashKey]; ok {
		if hashkey, ok = value.(string); !ok {
			return msg, nil
		}
	} else {
		hashkey = "default"
	}
	if exp, ok := rf.regexpList[hashkey]; ok {
		if exp.MatchString(message.(string)) {
			if rf.LabelName == "ignore" {
				return msg, fmt.Errorf("ignore")
			}
			(*msg)[fmt.Sprintf("%s_%s_RegexpCheck", rf.HashKey, rf.KeyToFilter)] = rf.LabelName
		}
	}
	return msg, nil
}

// Cleanup remove all
func (rf *RegexpFilter) Cleanup() {
}
