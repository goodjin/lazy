package main

import (
	"fmt"
	"github.com/jeromer/syslogparser/rfc3164"
	"strconv"
	"strings"
)

// LogSetting define log format setting

type LogSetting struct {
	LogType            string            `json:"logType"`
	SplitRegexp        string            `json:"splitRegexp,omitempty"`
	LogSource          string            `json:"logSource"`
	IndexTTL           string            `json:"indexTTL"`
	ElasticSearchHosts []string          `json:"elasticsearchHosts"`
	ElasticSearchHost  string            `json:"elasticsearchHost"`
	ElasticSearchPort  string            `json:"elasticsearchPort"`
	Tokens             []string          `json:"tokens,omitempty"`
	IgnoreTags         []string          `json:"ignoreTags,omitempty"`
	TokenFormat        map[string]string `json:"tokenFormat,omitempty"`
	AddtionCheck       []string          `json:"addtionCheck,omitempty"`
	hashedIgnoreTags   map[string]string
}

func (l *LogSetting) Parser(msg []byte) (map[string]interface{}, error) {
	data := make(map[string]interface{})
	var err error
	if l.LogType == "rfc3164" {
		p := rfc3164.NewParser(msg)
		if err = p.Parse(); err != nil {
			return data, err
		}
		data = p.Dump()
		tag := data["tag"].(string)
		tag = strings.Replace(tag, ".", "", -1)
		items := strings.Split(tag, "/")
		tag = items[len(items)-1]
		if len(tag) == 0 {
			tag = "misc"
		}
		data["tag"] = tag
	} else {
		data, err = l.wildFormat(generateLogTokens(msg))
	}
	return data, err
}

func (l *LogSetting) wildFormat(msgTokens []string) (map[string]interface{}, error) {
	data := make(map[string]interface{})
	if len(l.Tokens) != len(msgTokens) {
		return data, fmt.Errorf("log format error: %s %s", l.Tokens, msgTokens)
	}
	for i, token := range l.Tokens {
		tk := msgTokens[i]
		if format, ok := l.TokenFormat[token]; ok {
			switch format {
			case "int":
				t, err := strconv.ParseInt(tk, 10, 32)
				if err != nil {
					return data, fmt.Errorf("data format err: %s %s", tk, format)
				}
				data[token] = t
			case "strings":
				k := strings.Split(token, " ")
				v := strings.Split(string(tk), " ")
				if len(k) != len(v) {
					return data, fmt.Errorf("log fromat error: %s %s", k, v)
				}
				for l := 0; l < len(k); l++ {
					data[k[l]] = v[l]
				}
			case "float":
				t, err := strconv.ParseFloat(tk, 64)
				if err != nil {
					return data, fmt.Errorf("data format err: %s %s", tk, format)
				}
				data[token] = t
			default:
				data[token] = tk
			}
		}
	}
	return data, nil
}
