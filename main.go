package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	confFile = flag.String("c", "lazy.json", "lazy config file")
)

func main() {
	flag.Parse()
	logTaskConfig, err := ReadConfig(*confFile)
	if err != nil {
		log.Fatal("config parse error", err)
	}
	err = logTaskConfig.InitConfig()
	if err != nil {
		log.Fatal("failed to start pool error", err)
	}
	ticker := time.Tick(time.Second * 60)
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	taskPool := NewTaskPool()
	for {
		select {
		case <-ticker:
			topicsKey := fmt.Sprintf("%s/tasks", logTaskConfig.ConsulKey)
			tasksettings, err := logTaskConfig.ReadConfigFromConsul(topicsKey)
			if err != nil {
				log.Println(err)
			}
			taskPool.Cleanup(tasksettings)
			for k, v := range tasksettings {
				if taskPool.IsStarted(k) {
					continue
				}
				w := NewLogParserTask(k, []byte(v))
				if w == nil {
					continue
				}
				if err = w.Start(); err != nil {
					log.Println(k, v, err)
				} else {
					w.Stop()
				}
				taskPool.Join(w)
			}
		case <-termchan:
			taskPool.Stop()
			return
		}
	}
}
