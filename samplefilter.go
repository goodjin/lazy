package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

// config json
// {
// "SampleRateMod":"2", means count%2
// }

type SampleFilter struct {
	seed          *rand.Rand
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
	rf.seed = rand.New(rand.NewSource(time.Now().Unix()))
	return rf
}

func (rf *SampleFilter) Handle(msg *map[string]interface{}) (*map[string]interface{}, error) {
	current := rf.seed.Int()
	if current%rf.SampleRateMod > 0 {
		return msg, fmt.Errorf("ignore")
	}
	return msg, nil
}

func (rf *SampleFilter) Cleanup() {
}
