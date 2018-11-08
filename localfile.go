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
	fd       *os.File
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
	var err error
	m.fd, err = os.Open(m.FileName)
	if err != nil {
		return m, err
	}
	_, err = m.fd.Seek(0, io.SeekStart)
	if err != nil {
		return m, err
	}
	if !m.ReadAll {
		_, err = m.fd.Seek(0, io.SeekEnd)
		if err != nil {
			return m, err
		}
		log.Println("reading from EOF")
	}
	go m.ReadLoop()
	log.Println("start reading", m.FileName)
	return m, err
}

func (m *FileReader) ReadLoop() {
	reader := bufio.NewReader(m.fd)
	for {
		select {
		case <-m.exitChan:
			m.fd.Close()
			log.Println("closing reading", m.FileName)
			return
		default:
			current, _ := m.fd.Seek(0, io.SeekCurrent)
			line, err := reader.ReadBytes('\n')
			if err != nil && err != io.EOF {
				fmt.Println(err)
				time.Sleep(time.Second)
				break
			}
			if err == io.EOF {
				if len(line) == 0 {
					break
				}
				if line[len(line)-1] != byte('\n') {
					m.fd.Seek(current, io.SeekStart)
					reader = bufio.NewReader(m.fd)
					time.Sleep(time.Second)
					break
				}
			}
			if err == io.EOF {
				fd, err := os.Open(m.FileName)
				if err != nil {
					log.Println("open failed", err)
					break
				}
				m.fd.Close()
				neweof, err := fd.Seek(0, io.SeekEnd)
				if err != nil {
					log.Println(err)
				}
				if neweof < current {
					fd.Seek(0, io.SeekCurrent)
				} else {
					fd.Seek(current, io.SeekStart)
				}
				m.fd = fd
				reader = bufio.NewReader(fd)
			}
			if len(line) > 0 {
				logmsg := make(map[string][]byte)
				logmsg["msg"] = line
				m.msgChan <- &logmsg
			}
		}
	}
}

func (m *FileReader) Stop() {
	close(m.exitChan)
}
func (m *FileReader) GetMsgChan() chan *map[string][]byte {
	return m.msgChan
}
