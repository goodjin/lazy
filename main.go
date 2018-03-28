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
	taskPool := NewTaskPool()
	topicsKey := fmt.Sprintf("%s/tasks", logTaskConfig.ConsulKey)
	go func() {
		for {
			select {
			case <-ticker:
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
						w.Stop()
					}
					taskPool.Join(w)
				}
			}
		}
	}()
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	taskPool.Stop()
}
