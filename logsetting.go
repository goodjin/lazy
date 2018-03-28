package main

import (
	"fmt"
	"github.com/jbrukh/bayesian"
	"github.com/jeromer/syslogparser/rfc3164"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Proccesor support bayes or regexp match
// support mutli regexp rules, set matchtag if it matchs
// current matchtag is "ignore", "error", "critical", "warning"
type Proccesor struct {
	ProccesorType     string              `json:"ProccsorType"`
	Tag               string              `json:"Tag"`
	RegexpSetting     map[string]string   `json:"RegexpSetting,omitempty"`
	ClassifierSetting map[string][]string `json:"BayesClassifierSetting,omitempty"`
	exps              map[string]*regexp.Regexp
	c                 *bayesian.Classifier
	classifiers       []string
	WordSplitRegexp   string `json:"WordSplitRegexp,omitempty"`
	wordSplit         *regexp.Regexp
}

func (p *Proccesor) parseWords(msg string) []string {
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
func (p *Proccesor) Handler(msg *map[string]interface{}) {
	message := (*msg)[p.Tag]
	if p.ProccesorType == "Regexp" {
		for k, e := range p.exps {
			if e.MatchString(message.(string)) {
				(*msg)[fmt.Sprintf("%s_RegexpCheck", p.Tag)] = k
				if k == "ignore" {
					return
				}
			}
		}
		return
	}
	if p.ProccesorType == "Bayes" {
		if p.c == nil {
			return
		}
		words := p.parseWords(message.(string))
		_, likely, strict := p.c.LogScores(words)
		if strict {
			(*msg)[fmt.Sprintf("%s_BayesCheck", p.Tag)] = p.classifiers[likely]
		}
		return
	}
}

// LogSetting define logtask setting
type LogSetting struct {
	LogType     string            `json:"LogType"`
	TasksCount  int               `json:"TasksCount"`
	TimeZone    string            `json:"Timezone,omitempty"`
	TTL         string            `json:"TTL"`
	Config      map[string]string `json:"Config"`
	Tokens      []string          `json:"Tokens,omitempty"`
	IgnoreTags  []string          `json:"IgnoreTags,omitempty"`
	TokenFormat map[string]string `json:"TokenFormat,omitempty"`

	DispatchKey string                `json:"DispatchKey,omitempty"`
	Proccessors map[string]*Proccesor `json:"Proccessor,omitempty"`
}

func (l *LogSetting) SetRules() {
	for _, p := range l.Proccessors {
		if p.WordSplitRegexp != "" {
			p.wordSplit, _ = regexp.CompilePOSIX(p.WordSplitRegexp)
		}
		if p.ProccesorType == "Regexp" {
			p.exps = make(map[string]*regexp.Regexp)
			for k, v := range p.RegexpSetting {
				p.exps[k], _ = regexp.CompilePOSIX(v)
			}
			continue
		}
		if p.ProccesorType == "Bayes" {
			var classifierList []bayesian.Class
			for k := range p.ClassifierSetting {
				c := bayesian.Class(k)
				classifierList = append(classifierList, c)
			}
			p.c = bayesian.NewClassifier(classifierList...)
			for k, v := range p.ClassifierSetting {
				c := bayesian.Class(k)
				p.c.Learn(v, c)
			}
		}
	}
}

func (l *LogSetting) Parser(msg []byte) (*map[string]interface{}, error) {
	data := make(map[string]interface{})
	var err error
	if l.LogType == "rfc3164" {
		p := rfc3164.NewParser(msg)
		location, err := time.LoadLocation(l.TimeZone)
		if err == nil {
			p.Location(location)
		}
		if err = p.Parse(); err != nil {
			data["content"] = string(msg)
			data["timestamp"] = time.Now()
			return &data, nil
		}
		data = p.Dump()
		tag := data["tag"].(string)
		tag = strings.Replace(tag, ".", "", -1)
		items := strings.Split(tag, "/")
		tag = items[len(items)-1]
		if len(tag) == 0 {
			tag = "misc"
		}
		data["tag"] = strings.Trim(tag, "-")
	} else {
		return l.wildFormat(generateLogTokens(msg))
	}
	return &data, err
}

func (l *LogSetting) wildFormat(msgTokens []string) (*map[string]interface{}, error) {
	data := make(map[string]interface{})
	if len(l.Tokens) != len(msgTokens) {
		return &data, fmt.Errorf("log format error: %s %s", l.Tokens, msgTokens)
	}
	for i, token := range l.Tokens {
		tk := msgTokens[i]
		if format, ok := l.TokenFormat[token]; ok {
			switch format {
			case "int":
				t, err := strconv.ParseInt(tk, 10, 32)
				if err != nil {
					return &data, fmt.Errorf("data format err: %s %s", tk, format)
				}
				data[token] = t
			case "strings":
				k := strings.Split(token, " ")
				v := strings.Split(string(tk), " ")
				if len(k) != len(v) {
					return &data, fmt.Errorf("log fromat error: %s %s", k, v)
				}
				for l := 0; l < len(k); l++ {
					data[k[l]] = v[l]
				}
			case "float":
				t, err := strconv.ParseFloat(tk, 64)
				if err != nil {
					return &data, fmt.Errorf("data format err: %s %s", tk, format)
				}
				data[token] = t
			default:
				data[token] = tk
			}
		}
	}
	return &data, nil
}
