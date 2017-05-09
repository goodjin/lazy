package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	confFile = flag.String("c", "lazy.json", "lazy config file")
)

func main() {
	flag.Parse()
	c, err := ReadConfig(*confFile)
	if err != nil {
		log.Fatal("config parse error", err)
	}
	var logParserPool *LogParserPool
	logParserPool = &LogParserPool{
		Setting:       c,
		exitChannel:   make(chan int),
		logParserList: make(map[string]*LogParser),
	}
	err = logParserPool.Run()
	if err != nil {
		log.Fatal("failed to start pool error", err)
	}
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	logParserPool.Stop()
}
