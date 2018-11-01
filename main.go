package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
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
	topicsKey := fmt.Sprintf("%s/tasks", logTaskConfig.ConsulKey)
	taskParallel := runtime.NumCPU() / 2
	if taskParallel < 2 {
		taskParallel = 2
	}
	for {
		select {
		case <-ticker:
			tasksettings, err := logTaskConfig.ReadConfigFromConsul(topicsKey)
			if err != nil {
				log.Println(err)
			} else {
				taskPool.Cleanup(tasksettings)
			}
			for k, v := range tasksettings {
				if taskPool.IsStarted(k) {
					continue
				}
				w, err := NewLogProcessTask(k, []byte(v))
				if err != nil {
					fmt.Println(err, v)
					continue
				}
				for i := 0; i < taskParallel; i++ {
					go w.Run()
				}
				taskPool.Join(w)
				fmt.Println(k, " is started")
			}
		case <-termchan:
			taskPool.Stop()
			return
		}
	}
}
