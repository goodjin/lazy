package main

import (
	"fmt"
	"github.com/go-kit/kit/metrics/statsd"
	"regexp"
)

// config json
// {
// "TagToFilter":"syslogtag",
// "a":"(.*)",
// "b":"good,bad",
// "ignore":"a,b,c",
// }

type RegexpFilter struct {
	TagToFilter   string            `json:"TagToFilter"`
	RegexpSetting map[string]string `json:"RegexpSetting,omitempty"`
	regexpList    map[string]*regexp.Regexp
	statsd        *statsd.Statsd
}

func NewRegexpFilter(config map[string]string) *RegexpFilter {
	rf := &RegexpFilter{
		TagToFilter: config["TagToFilter"],
	}
	rf.RegexpSetting = make(map[string]string)
	delete(config, "TagToFilter")
	rf.regexpList = make(map[string]*regexp.Regexp)
	for k, v := range config {
		rf.regexpList[k], _ = regexp.CompilePOSIX(v)
	}
	return rf
}

func (rf *RegexpFilter) Handle(msg *map[string]interface{}) (*map[string]interface{}, error) {
	message := (*msg)[rf.TagToFilter]
	filterState := rf.statsd.NewCounter("regexpfilter_count", 1.0)
	filterState.Add(1)
	for k, exp := range rf.regexpList {
		if exp.MatchString(message.(string)) {
			(*msg)[fmt.Sprintf("%s_RegexpCheck", rf.TagToFilter)] = k
			if k == "ignore" {
				filterignoreState := rf.statsd.NewCounter("regexpfilter_ignore", 1.0)
				filterignoreState.Add(1)
				return msg, fmt.Errorf("ignore")
			}
		}
	}
	return msg, nil
}

func (rf *RegexpFilter) Cleanup() {
}
func (rf *RegexpFilter) SetStatsd(statsd *statsd.Statsd) {
	rf.statsd = statsd
}
