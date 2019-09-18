package main

import (
	"fmt"
	"strconv"
	"sync/atomic"
)

// config json
// {
// "SampleRateMod":"2", means count%2
// }

type SampleFilter struct {
	count         int64
	SampleRateMod int `json:"SampleRateMod"`
}

func NewSampleFilter(config map[string]string) *SampleFilter {
	rate, err := strconv.Atoi(config["SampleRateMod"])
	if err != nil {
		rate = 1
	}
	rf := &SampleFilter{
		SampleRateMod: rate,
	}
	rf.count = 0
	return rf
}

func (rf *SampleFilter) Handle(msg *map[string]interface{}) (*map[string]interface{}, error) {
	atomic.AddInt64(&rf.count, 1)
	if atomic.LoadInt64(&rf.count) < int64(rf.SampleRateMod) {
		return msg, fmt.Errorf("ignore")
	}
	atomic.StoreInt64(&rf.count, 0)
	return msg, nil
}

func (rf *SampleFilter) Cleanup() {
}
