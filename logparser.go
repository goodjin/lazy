package main

import (
	"encoding/json"
	"fmt"
	"github.com/jeromer/syslogparser/rfc3164"
	"strconv"
	"strings"
	"time"
)

type LogParser struct {
	LogType     string            `json:"LogType"`
	TimeZone    string            `json:"Timezone,omitempty"`
	Tokens      []string          `json:"Tokens,omitempty"`
	TokenFormat map[string]string `json:"TokenFormat,omitempty"`
}

func (l *LogParser) Handle(msg *map[string][]byte) (*map[string]interface{}, error) {
	data := make(map[string]interface{})
	var err error
	switch l.LogType {
	case "rfc3164":
		p := rfc3164.NewParser((*msg)["msg"])
		location, err := time.LoadLocation(l.TimeZone)
		if err == nil {
			p.Location(location)
		}
		if err = p.Parse(); err != nil {
			data["content"] = string((*msg)["msg"])
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
		data["from"] = string((*msg)["from"])
	case "customschema":
		return l.wildFormat(generateLogTokens((*msg)["msg"]))
	case "keyvalue":
		var kv map[string]string
		err = json.Unmarshal((*msg)["msg"], &kv)
		if err == nil {
			for k, v := range kv {
				data[k] = v
				if k == "timestamp" {
					data["RawTimestamp"] = v
				}
			}
			data["timestamp"] = time.Now()
		}
	default:
		data["timestamp"] = time.Now()
		data["rawmsg"] = string((*msg)["msg"])
		return &data, nil
	}
	return &data, err
}

func (l *LogParser) wildFormat(msgTokens *[]string) (*map[string]interface{}, error) {
	data := make(map[string]interface{})
	if len(l.Tokens) != len(*msgTokens) {
		return &data, fmt.Errorf("log format error: %s %s", l.Tokens, *msgTokens)
	}
	for i, token := range l.Tokens {
		tk := (*msgTokens)[i]
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
				var trimedArray []string
				if len(k) != len(v) {
					for _, str := range v {
						if str != "" {
							trimedArray = append(trimedArray, str)
						}
					}
					v = trimedArray
					if len(k) != len(trimedArray) {
						return &data, fmt.Errorf("log fromat error: %s %s", k, v)
					}
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
			case "nginxtimestamp":
				var err error
				data[token], err = time.Parse("02/Jan/2006:15:04:05 -0700", tk)
				if err != nil {
					return &data, fmt.Errorf("data format err: %s %s", tk, format)
				}
			default:
				data[token] = tk
			}
		}
	}
	if _, ok := data["timestamp"]; !ok {
		data["timestamp"] = time.Now()
	}
	return &data, nil
}

func generateLogTokens(buf []byte) *[]string {
	var tokens []string
	var token []byte
	var lastChar byte
	for _, v := range buf {
		switch v {
		case byte(' '):
			fallthrough
		case byte('['):
			fallthrough
		case byte(']'):
			fallthrough
		case byte('"'):
			if len(token) > 0 {
				if token[len(token)-1] == byte('\\') {
					token = append(token, v)
					continue
				}
				if lastChar == byte('"') {
					if v != byte('"') {
						token = append(token, v)
						continue
					}
				}
				if lastChar == byte('[') {
					if v != byte(']') {
						token = append(token, v)
						continue
					}
				}
				tokens = append(tokens, string(token))
				token = make([]byte, 0)
			} else {
				if lastChar == byte('"') {
					if v == byte('"') {
						tokens = append(tokens, string(token))
						token = make([]byte, 0)
					}
				}
				if lastChar == byte('[') {
					if v == byte(']') {
						tokens = append(tokens, string(token))
						token = make([]byte, 0)
					}
				}
			}
			lastChar = v
		default:
			token = append(token, v)
		}
	}
	return &tokens
}
