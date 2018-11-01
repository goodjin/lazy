package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

// config
// {
// "FileName":"./xxx",
// "ReadAll":"true",
// "Type":"file"
// }

type FileReader struct {
	FileName string
	ReadAll  bool
	exitChan chan int
	msgChan  chan *map[string][]byte
}

func NewFileReader(config map[string]string) (*FileReader, error) {
	m := &FileReader{}
	m.msgChan = make(chan *map[string][]byte)
	m.exitChan = make(chan int)
	m.FileName = config["FileName"]
	m.ReadAll = false
	if config["ReadAll"] == "true" {
		m.ReadAll = true
	}
	fd, err := os.Open(m.FileName)
	if err != nil {
		return m, err
	}
	defer fd.Close()
	_, err = fd.Seek(0, io.SeekStart)
	if err != nil {
		return m, err
	}
	if !m.ReadAll {
		_, err = fd.Seek(0, io.SeekEnd)
		if err != nil {
			return m, err
		}
		log.Println("reading from EOF")
	}
	log.Println("reading ", m.FileName)
	logmsg := make(map[string][]byte)
	go func() {
		reader := bufio.NewReader(fd)
		for {
			select {
			case <-m.exitChan:
				return
			default:
				line, err := reader.ReadString('\n')
				if err != nil && err != io.EOF {
					fmt.Println(err)
					time.Sleep(time.Second)
					break
				}
				if err == io.EOF {
					size0, err := fd.Seek(0, io.SeekCurrent)
					if err != nil {
						log.Println("open failed", err)
						break
					}
					fd, err = os.Open(m.FileName)
					if err != nil {
						log.Println("open failed", err)
						break
					}
					size1, err := fd.Seek(0, io.SeekEnd)
					if err != nil {
						log.Println(err)
					}
					if size1 < size0 {
						fd.Seek(0, io.SeekCurrent)
					} else {
						fd.Seek(size0, io.SeekStart)
					}
					reader = bufio.NewReader(fd)
				}
				if len(line) > 0 {
					logmsg["msg"] = []byte(line)
				}
				m.msgChan <- &logmsg
			}
		}
	}()
	return m, err
}

func (m *FileReader) Stop() {
	close(m.exitChan)
}
func (m *FileReader) GetMsgChan() chan *map[string][]byte {
	return m.msgChan
}
