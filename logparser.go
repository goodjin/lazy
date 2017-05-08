package main

import (
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/consul/api"
	"github.com/jbrukh/bayesian"
	"github.com/mattbaird/elastigo/lib"
	"github.com/nsqio/go-nsq"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

type RegexpSetting struct {
	E   string         `json:"regexp"`
	Exp *regexp.Regexp `json:"-"`
	TTL string         `json:"ttl"`
}

type LogParser struct {
	sync.RWMutex
	bayesLock sync.Mutex
	*Setting
	logTopic        string
	consumer        *nsq.Consumer
	producer        *nsq.Producer
	logSetting      *LogSetting
	classifiers     []string
	c               *bayesian.Classifier
	wordSplitRegexp *regexp.Regexp
	client          *api.Client
	regexMap        map[string][]*RegexpSetting
	exitChannel     chan int
	msgChannel      chan ElasticRecord
}

func (m *LogParser) Run() error {
	config := api.DefaultConfig()
	config.Address = m.ConsulAddress
	config.Datacenter = m.Datacenter
	config.Token = m.Token
	var err error
	m.client, err = api.NewClient(config)
	if err != nil {
		return err
	}
	m.getRegexp()
	m.getBayes()
	m.wordSplitRegexp = regexp.MustCompile(m.logSetting.SplitRegexp)
	for i := 0; i < m.MaxInFlight/10+1; i++ {
		go m.elasticSearchBuildIndex()
	}
	cfg := nsq.NewConfig()
	hostname, err := os.Hostname()
	cfg.Set("user_agent", fmt.Sprintf("lazy/%s", hostname))
	cfg.Set("snappy", true)
	cfg.Set("max_in_flight", m.MaxInFlight)
	m.producer, err = nsq.NewProducer(m.NsqdAddress, cfg)
	m.consumer, err = nsq.NewConsumer(m.logSetting.LogSource, m.Setting.LogChannel, cfg)
	if err != nil {
		log.Println(m.logSetting.LogSource, err)
		return err
	}
	m.consumer.AddConcurrentHandlers(m, m.MaxInFlight)
	err = m.consumer.ConnectToNSQLookupds(m.LookupdAddresses)
	if err != nil {
		return err
	}
	go m.syncLogFormat()
	return err
}

func (m *LogParser) Stop() {
	m.consumer.Stop()
	m.producer.Stop()
	close(m.exitChannel)
}

func (m *LogParser) HandleMessage(msg *nsq.Message) error {
	var logFormat LogFormat
	var msglog string
	m.RLock()
	logSetting := m.logSetting
	regexpMap := m.regexMap
	classifiers := m.classifiers
	m.RUnlock()
	if logSetting.LogType == "rfc3164" {
		err := proto.Unmarshal(msg.Body, &logFormat)
		if err != nil {
			log.Println(err)
			return nil
		}
		msglog = logFormat.GetRawmsg()
		if len(msglog) < 1 {
			return nil
		}
	} else {
		msglog = string(msg.Body)
	}
	record := ElasticRecord{
		errChannel: make(chan error),
		ttl:        logSetting.IndexTTL,
	}
	message, err := logSetting.Parser([]byte(msglog))
	if logSetting.LogType == "rfc3164" {
		message["from"] = logFormat.From
	} else {
		message["timestamp"] = time.Now()
	}
	if err != nil {
		log.Println(err, msglog)
		return nil
	}
	if logSetting.LogType == "rfc3164" {
		tag := message["tag"].(string)
		if _, ok := logSetting.hashedIgnoreTags[tag]; ok {
			return nil
		}
		for _, check := range logSetting.AddtionCheck {
			switch check {
			case "regexp":
				rg, ok := regexMap[tag]
				if ok {
					message["ttl"] = "-1"
					for _, r := range rg {
						if r.Exp.MatchString(message["content"].(string)) {
							message["ttl"] = r.TTL
							record.ttl = r.TTL
						}
					}
				}
			case "bayes":
				words := m.parseWords(message["content"].(string))
				if m.c == nil {
					continue
				}
				m.bayesLock.Lock()
				_, likely, strict := m.c.LogScores(words)
				m.bayesLock.Unlock()
				message["bayes_check"] = "undefined"
				if strict {
					message["bayes_check"] = classifiers[likely]
				}
			default:
				log.Println("unsupportted check way", check)
			}
		}
		if message["ttl"] == 1 {
			return nil
		}
	}
	if message["bayes_check"] == "undefined" {
		m.producer.Publish(m.TrainTopic, msg.Body)
	}
	record.body = message
	m.msgChannel <- record
	return <-record.errChannel
}

func (m *LogParser) syncLogFormat() {
	if err := m.getLogFormat(); err != nil {
		log.Println(err)
	}
	ticker := time.Tick(time.Second * 60)
	for {
		select {
		case <-ticker:
			if err := m.getLogFormat(); err != nil {
				log.Println(err)
				continue
			}
			for _, check := range m.logSetting.AddtionCheck {
				switch check {
				case "regexp":
					if err := m.getRegexp(); err != nil {
						log.Println(err)
					}
				case "bayes":
					if err := m.getBayes(); err != nil {
						log.Println(err)
					}
				default:
					log.Println("unsupportted check way", check)
				}
			}
		case <-m.exitChannel:
			return
		}
	}
}

func (m *LogParser) elasticSearchBuildIndex() {
	c := elastigo.NewConn()
	m.RLock()
	c.SetHosts(m.logSetting.ElasticSearchHosts)
	logsource := m.logSetting.LogSource
	logtype := m.logSetting.LogType
	m.RUnlock()
	indexor := c.NewBulkIndexerErrors(10, 60)
	ticker := time.Tick(time.Second * 600)
	yy, mm, dd := time.Now().Date()
	indexPatten := fmt.Sprintf("-%d.%d.%d", yy, mm, dd)
	indexor.Start()
	defer indexor.Stop()
	defer c.Close()
	var err error
	searchIndex := logsource + indexPatten
	for {
		timestamp := time.Now()
		select {
		case <-ticker:
			yy, mm, dd = timestamp.Date()
			indexPatten = fmt.Sprintf("-%d.%d.%d", yy, mm, dd)
			searchIndex = logsource + indexPatten
		case errBuf := <-indexor.ErrorChannel:
			log.Println(errBuf.Err)
		case r := <-m.msgChannel:
			err = indexor.Index(searchIndex, logtype, "", "", fmt.Sprintf("%ss", r.ttl), &timestamp, r.body)
			r.errChannel <- err
		case <-m.exitChannel:
			log.Println("exit elasticsearch")
			return
		}
	}
}

func (m *LogParser) parseWords(msg string) []string {
	t := strings.Split(m.wordSplitRegexp.ReplaceAllString(msg, " "), " ")
	var tokens []string
	for _, v := range t {
		tokens = append(tokens, strings.ToLower(v))
	}
	return tokens
}

func (m *LogParser) getLogFormat() error {
	kv := m.client.KV()
	topicsKey := fmt.Sprintf("%s/topics/%s", m.ConsulKey, m.logTopic)
	value, _, err := kv.Get(topicsKey, nil)
	if err != nil {
		return err
	}
	var logSetting LogSetting
	err = json.Unmarshal(value.Value, &logSetting)
	if err != nil {
		return err
	}
	logSetting.hashedIgnoreTags = make(map[string]string)
	for _, v := range logSetting.IgnoreTags {
		logSetting.hashedIgnoreTags[v] = v
	}
	m.Lock()
	defer m.Unlock()
	m.logSetting = &logSetting
	return nil
}

func (m *LogParser) getBayes() error {
	kv := m.client.KV()
	key := fmt.Sprintf("%s/classifiers/%s", m.ConsulKey, m.logTopic)
	classifiers, _, err := kv.List(key, nil)
	if err != nil {
		return err
	}

	if len(classifiers) < 2 {
		return fmt.Errorf("%s", "classifiers is less than 2")
	}
	var classifierList []bayesian.Class
	size := len(key) + 1
	var clist []string
	for _, value := range classifiers {
		if len(value.Key) <= size {
			continue
		}
		c := bayesian.Class(value.Key[size:])
		clist = append(clist, value.Key[size:])
		classifierList = append(classifierList, c)
	}
	m.Lock()
	defer m.Unlock()
	m.c = bayesian.NewClassifier(classifierList...)
	for _, value := range classifiers {
		if len(value.Key) <= size {
			continue
		}
		c := bayesian.Class(value.Key[size:])
		words := strings.Split(string(value.Value), ",")
		m.c.Learn(words, c)
	}
	m.classifiers = clist
	return nil
}

func (m *LogParser) getRegexp() error {
	kv := m.client.KV()
	key := fmt.Sprintf("%s/regexp/%s", m.ConsulKey, m.logTopic)
	pairs, _, err := kv.List(key, nil)
	if err != nil {
		return err
	}
	m.Lock()
	defer m.Unlock()
	size := len(m.ConsulKey) + 1
	for _, value := range pairs {
		if len(value.Key) > size {
			var regs []*RegexpSetting
			if err := json.Unmarshal(value.Value, &regs); err == nil {
				for i, v := range regs {
					x, e := regexp.CompilePOSIX(v.E)
					if e != nil {
						log.Println("get regexp", e)
						continue
					}
					regs[i].Exp = x
				}
				m.regexMap[value.Key[size:]] = regs
			}
		}
	}
	return nil
}
