package main

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/jbrukh/bayesian"
)

// config json
// {
// "KeyToFilter":"body",
// "WordSplitRegexp":"xxxx",
// "Classifiers":"good,bad",
// "good":"a,b,c",
// "bad":"c,d,e",
// }

// BayiesFilter bayies filter
type BayiesFilter struct {
	KeyToFilter     string `json:"KeyToFilter"`
	WordSplitRegexp string `json:"WordSplitRegexp,omitempty"`
	wordSplit       *regexp.Regexp
	c               *bayesian.Classifier
	classifiers     []string
}

// NewBayiesFilter create new BayiesFilter
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

// Handle handle msg
func (p *BayiesFilter) Handle(msg *map[string]interface{}) (*map[string]interface{}, error) {
	message := (*msg)[p.KeyToFilter]
	if p.c == nil {
		return msg, fmt.Errorf("no bayies config")
	}
	if len(message.(string)) == 0 {
		return msg, fmt.Errorf("ignore")
	}
	words := p.parseWords(message.(string))
	_, likely, strict := p.c.LogScores(words)
	if strict {
		(*msg)[fmt.Sprintf("%s_BayesCheck", p.KeyToFilter)] = p.classifiers[likely]
	}
	return msg, nil
}

// Cleanup close all
func (p *BayiesFilter) Cleanup() {
}
