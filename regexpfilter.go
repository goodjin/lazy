package main

import (
	"fmt"
	"regexp"

	"github.com/prometheus/client_golang/prometheus"
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
	metricstatus  *prometheus.CounterVec
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
	var err error
	for k, v := range config {
		rf.regexpList[k], err = regexp.CompilePOSIX(v)
		if err != nil {
			delete(rf.regexpList, k)
			fmt.Println(k, v, err)
		}
	}
	rf.metricstatus = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "regexp_filter",
			Help: "regexp filter status.",
		},
		[]string{"rule", "count"},
	)
	// Register status
	prometheus.Register(rf.metricstatus)
	return rf
}

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
				rf.metricstatus.WithLabelValues(rf.HashKey, "ignore").Inc()
				return msg, fmt.Errorf("ignore")
			}
			(*msg)[fmt.Sprintf("%s_%s_RegexpCheck", rf.HashKey, rf.KeyToFilter)] = rf.LabelName
			rf.metricstatus.WithLabelValues(rf.HashKey, rf.LabelName).Inc()
		}
	}
	return msg, nil
}

func (rf *RegexpFilter) Cleanup() {
}
