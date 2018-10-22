package main

import (
	"fmt"
	"github.com/go-kit/kit/metrics/statsd"
	"regexp"
)

// config json
// {
// "KeyToFilter":"syslogtag",
// "HashKey":"tag",
// "b":"good,bad",
// "ignore":"a,b,c",
// }

type RegexpFilter struct {
	HashKey       string            `json:"HashKey"`
	KeyToFilter   string            `json:"KeyToFilter"`
	LabelName     string            `json:"LabelName"`
	RegexpSetting map[string]string `json:"RegexpSetting,omitempty"`
	regexpList    map[string]*regexp.Regexp
	statsd        *statsd.Statsd
}

func NewRegexpFilter(config map[string]string) *RegexpFilter {
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
	for k, v := range config {
		rf.regexpList[k], _ = regexp.CompilePOSIX(v)
	}
	return rf
}

func (rf *RegexpFilter) Handle(msg *map[string]interface{}) (*map[string]interface{}, error) {
	message := (*msg)[rf.KeyToFilter]
	filterState := rf.statsd.NewCounter(fmt.Sprintf("%s_%s_regexp_count", rf.HashKey, rf.KeyToFilter), 1.0)
	filterState.Add(1)
	var hashkey string
	if value, ok := (*msg)[rf.HashKey]; ok {
		if hashkey, ok = value.(string); !ok {
			return msg, nil
		}
	} else {
		hashkey = "default"
	}
	exp := rf.regexpList[hashkey]
	if exp.MatchString(message.(string)) {
		if rf.LabelName == "ignore" {
			return msg, fmt.Errorf("ignore")
		}
		(*msg)[fmt.Sprintf("%s_%s_RegexpCheck", rf.HashKey, rf.KeyToFilter)] = rf.LabelName
	}
	return msg, nil
}

func (rf *RegexpFilter) Cleanup() {
}
func (rf *RegexpFilter) SetStatsd(statsd *statsd.Statsd) {
	rf.statsd = statsd
}
