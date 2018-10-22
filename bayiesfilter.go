package main

import (
	"fmt"
	"github.com/go-kit/kit/metrics/statsd"
	"github.com/jbrukh/bayesian"
	"regexp"
	"strings"
)

// config json
// {
// "KeyToFilter":"body",
// "WordSplitRegexp":"xxxx",
// "Classifiers":"good,bad",
// "good":"a,b,c",
// "bad":"c,d,e",
// }
type BayiesFilter struct {
	KeyToFilter     string `json:"KeyToFilter"`
	WordSplitRegexp string `json:"WordSplitRegexp,omitempty"`
	wordSplit       *regexp.Regexp
	c               *bayesian.Classifier
	statsd          *statsd.Statsd
	classifiers     []string
}

func NewBayiesFilter(config map[string]string) *BayiesFilter {
	bf := &BayiesFilter{
		KeyToFilter:     config["KeyToFilter"],
		WordSplitRegexp: config["WordSplitRegexp"],
	}
	if len(bf.WordSplitRegexp) > 0 {
		bf.wordSplit, _ = regexp.CompilePOSIX(bf.WordSplitRegexp)
	}
	var classifierList []bayesian.Class
	for _, v := range strings.Split(config["Classifiers"], ",") {
		k := strings.TrimSpace(v)
		c := bayesian.Class(k)
		bf.classifiers = append(bf.classifiers, k)
		classifierList = append(classifierList, c)
	}
	bf.c = bayesian.NewClassifier(classifierList...)
	for _, k := range bf.classifiers {
		c := bayesian.Class(k)
		values := strings.Split(config[k], ",")
		bf.c.Learn(values, c)
	}
	return bf
}

func (p *BayiesFilter) parseWords(msg string) []string {
	var t []string
	if p.wordSplit != nil {
		t = strings.Split(p.wordSplit.ReplaceAllString(msg, " "), " ")
	} else {
		t = strings.Split(msg, " ")
	}
	var tokens []string
	for _, v := range t {
		tokens = append(tokens, strings.ToLower(v))
	}
	return tokens
}

func (p *BayiesFilter) Handle(msg *map[string]interface{}) (*map[string]interface{}, error) {
	message := (*msg)[p.KeyToFilter]
	if p.c == nil {
		return msg, fmt.Errorf("no bayies config")
	}
	words := p.parseWords(message.(string))
	_, likely, strict := p.c.LogScores(words)
	if strict {
		(*msg)[fmt.Sprintf("%s_BayesCheck", p.KeyToFilter)] = p.classifiers[likely]
	}
	filterState := p.statsd.NewCounter("bayiesfilter_ignore", 1.0)
	filterState.Add(1)
	return msg, nil
}
func (p *BayiesFilter) Cleanup() {
}
func (p *BayiesFilter) SetStatsd(statsd *statsd.Statsd) {
	p.statsd = statsd
}
