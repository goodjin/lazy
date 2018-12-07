package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/owulveryck/lstm"
	"github.com/owulveryck/lstm/datasetter/char"
	"io"
	"io/ioutil"
	"os"
	"strconv"
)

// config json
// {
// "SampleSize":"100",
// "VocabularyFile":"./vocabularyfile.txt",
// "ModelFile":"./models.m"
// }

type LSTMFilter struct {
	vocabSize       int
	model           *lstm.Model
	vocabulary      []rune
	vocabularyTable map[rune]int
	HiddenSize      int    `json:"HiddenSize"`
	SampleSize      int    `json:"SampleSize"`
	VocabularyFile  string `json:"VocabularyFile"`
	SampleFile      string `json:"SampleFile"`
	ModelFile       string `json:"ModelFile"`
}

func NewLSTMFilter(config map[string]string) *LSTMFilter {
	lstmfilter := &LSTMFilter{
		VocabularyFile: config["VocabularyFile"],
		ModelFile:      config["ModelFile"],
	}
	var err error
	lstmfilter.SampleSize, err = strconv.Atoi(config["SampleSize"])
	if err != nil {
		lstmfilter.SampleSize = 100
	}
	lstmfilter.HiddenSize, err = strconv.Atoi(config["HiddenSize"])
	if err != nil {
		lstmfilter.HiddenSize = 100
	}
	lstmfilter.newVocabulary()
	// input, output, hiddensize
	lstmfilter.model = lstm.NewModel(lstmfilter.vocabSize, lstmfilter.vocabSize, lstmfilter.HiddenSize)
	lstmfilter.RecoverModel()
	return lstmfilter
}

func (lstmfilter *LSTMFilter) newVocabulary() error {
	f, err := os.Open(lstmfilter.VocabularyFile)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	i := 0
	lstmfilter.vocabularyTable = make(map[rune]int)
	for {
		if c, _, err := r.ReadRune(); err != nil {
			if err == io.EOF {
				break
			}
			return err
		} else {
			if _, ok := lstmfilter.vocabularyTable[c]; !ok {
				lstmfilter.vocabularyTable[c] = i
				lstmfilter.vocabulary = append(lstmfilter.vocabulary, c)
				i++
			}
		}
	}
	lstmfilter.vocabSize = len(lstmfilter.vocabulary)
	return nil
}

// file mode
func (lstmfilter *LSTMFilter) RecoverModel() error {
	f, err := os.OpenFile(lstmfilter.ModelFile, os.O_RDWR|os.O_CREATE, 0644)
	body, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}
	f.Close()
	return lstmfilter.model.UnmarshalBinary(body)

}

func (lstmfilter *LSTMFilter) runeToIndex(r rune) (int, error) {
	if _, ok := lstmfilter.vocabularyTable[r]; !ok {
		return 0, fmt.Errorf("Rune %v is not part of the vocabulary", string(r))
	}
	return lstmfilter.vocabularyTable[r], nil
}

func (lstmfilter *LSTMFilter) indexToRune(i int) (rune, error) {
	if i >= len(lstmfilter.vocabulary) {
		return 0, fmt.Errorf("index invalid, no rune references")
	}
	return lstmfilter.vocabulary[i], nil
}

func (lstmfilter *LSTMFilter) Handle(msg *map[string]interface{}) (*map[string]interface{}, error) {
	var rawmsg string
	for _, v := range *msg {
		rawmsg = fmt.Sprintf("%s %v", rawmsg, v)
	}
	prediction := char.NewPrediction(rawmsg, lstmfilter.runeToIndex, lstmfilter.SampleSize, lstmfilter.vocabSize)
	err := lstmfilter.model.Predict(context.TODO(), prediction)
	if err != nil {
		return msg, err
	}
	var rst string
	for _, output := range prediction.GetOutput() {
		var idx int
		for i, val := range output {
			if val == 1 {
				idx = i
			}
		}
		rne, err := lstmfilter.indexToRune(idx)
		if err != nil {
			return msg, err
		}
		rst = fmt.Sprintf("%s%s", rst, string(rne))
	}
	(*msg)["MachineCheck"] = rst
	return msg, nil
}

func (lstmfilter *LSTMFilter) Cleanup() {
}