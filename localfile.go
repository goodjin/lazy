package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"
)

// config
// {
// "Files":"./xxx",
// "ReadAll":"true",
// "Type":"file"
// }

type FileExInfo struct {
	fd       *os.File
	Setting  *FileReader
	exitChan chan int
	offsize  int64
	Name     string `json:"Name"`
	Inode    uint64 `json:"Inode"`
	Device   uint64 `json:"Device"`
}

func GetFileExInfo(info os.FileInfo) (uint64, uint64) {
	stat := info.Sys().(*syscall.Stat_t)

	return uint64(stat.Ino), uint64(stat.Dev)
}

func (fs FileExInfo) IsSame(fInfo FileExInfo) bool {
	return fs.Inode == fInfo.Inode && fs.Device == fInfo.Device
}

func (fs FileExInfo) GetHashString() string {
	return fmt.Sprintf("%d:%d", fs.Inode, fs.Device)
}

func NewFileExInfo(name string, freader *FileReader) (*FileExInfo, error) {
	fInfo := &FileExInfo{Name: name}
	fInfo.Setting = freader
	var err error
	fInfo.fd, err = os.Open(fInfo.Name)
	if err != nil {
		return fInfo, err
	}
	fstat, err := fInfo.fd.Stat()
	if err == nil {
		fInfo.Inode, fInfo.Device = GetFileExInfo(fstat)
	}
	return fInfo, err
}
func (m *FileExInfo) ReadLoop() {
	if m.Setting.ReadAll {
		m.offsize, _ = m.fd.Seek(0, io.SeekStart)
	} else {
		m.offsize, _ = m.fd.Seek(0, io.SeekEnd)
	}
	reader := bufio.NewReader(m.fd)
	for {
		select {
		case <-m.exitChan:
			return
		default:
			m.offsize, _ = m.fd.Seek(0, io.SeekCurrent)
			line, err := reader.ReadBytes('\n')
			if err != nil && err != io.EOF {
				fmt.Println(err)
				if len(line) > 0 {
					m.fd.Seek(m.offsize, io.SeekStart)
				}
				time.Sleep(time.Second)
				break
			}
			if err == io.EOF {
				// check same name is rename or not
				fInfo, err := NewFileExInfo(m.Name, m.Setting)
				if err == nil {
					if !m.IsSame(*fInfo) {
						// renamed
						m.Setting.refreshChan <- 1
					}
					fInfo.Stop()
				}
				if len(line) == 0 {
					time.Sleep(time.Second)
					break
				}
				if line[len(line)-1] != byte('\n') {
					// reset readline
					m.fd.Seek(m.offsize, io.SeekStart)
					reader = bufio.NewReader(m.fd)
					time.Sleep(time.Second)
					break
				}
			}
			if len(line) > 0 {
				logmsg := make(map[string][]byte)
				logmsg["msg"] = line
				m.Setting.msgChan <- &logmsg
			}
		}
	}
}
func (fs FileExInfo) Stop() {
	close(fs.exitChan)
	fs.fd.Close()
}

type FileReader struct {
	sync.Mutex
	Files       map[string]*FileExInfo
	msgChan     chan *map[string][]byte
	refreshChan chan int
	FileList    string
	ReadAll     bool
	exitChan    chan int
}

func NewFileReader(config map[string]string) (*FileReader, error) {
	m := &FileReader{}
	m.exitChan = make(chan int)
	m.refreshChan = make(chan int)
	m.Files = make(map[string]*FileExInfo)
	m.FileList = config["Files"]
	m.ReadAll = false
	if config["ReadAll"] == "true" {
		m.ReadAll = true
	}
	err := m.GetFiles()
	if err == nil {
		m.ReadAll = true
	}
	go func() {
		ticker := time.Tick(time.Minute)
		for {
			select {
			case <-ticker:
				go m.GetFiles()
			case <-m.refreshChan:
				go m.GetFiles()
			case <-m.exitChan:
				return
			}
		}
	}()
	return m, err
}

func (m *FileReader) GetFiles() error {
	tokens := strings.Split(m.FileList, "/")
	fileName := tokens[len(tokens)-1]
	var path string
	if len(tokens) > 2 {
		path = strings.Join(tokens[:len(tokens)-2], "/")
	} else {
		path = "."
	}
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}
	m.Lock()
	fileMap := make(map[string]string)
	exactFile, err := NewFileExInfo(m.FileList, m)
	if err == nil {
		fileMap[exactFile.GetHashString()] = exactFile.Name
		f, ok := m.Files[exactFile.GetHashString()]
		if ok {
			if f.Name != exactFile.Name {
				f.Name = exactFile.Name
			}
			exactFile.fd.Close()
		} else {
			m.Files[exactFile.GetHashString()] = exactFile
			log.Println("start reading", exactFile.Name)
			go exactFile.ReadLoop()
		}
	} else {
		for _, file := range files {
			reg, err := regexp.Compile(fileName)
			if err != nil {
				m.Unlock()
				return err
			}
			if !reg.MatchString(file.Name()) {
				continue
			}
			fInfo, err := NewFileExInfo(fmt.Sprintf("%s/%s", path, file.Name()), m)
			if err == nil {
				fileMap[fInfo.GetHashString()] = fInfo.Name
				f, ok := m.Files[fInfo.GetHashString()]
				if ok {
					if f.Name != fInfo.Name {
						f.Name = fInfo.Name
					}
					fInfo.fd.Close()
					continue
				}
				m.Files[fInfo.GetHashString()] = fInfo
				log.Println("start reading", fInfo.Name)
				go fInfo.ReadLoop()
			}
		}
	}
	for _, v := range m.Files {
		if _, ok := fileMap[v.GetHashString()]; !ok {
			m.Files[v.GetHashString()].Stop()
			delete(m.Files, v.GetHashString())
		}
	}
	m.Unlock()
	return nil
}
func (m *FileReader) Stop() {
	close(m.exitChan)
	m.Lock()
	for _, file := range m.Files {
		file.Stop()
	}
	m.Unlock()
}
func (m *FileReader) GetMsgChan() chan *map[string][]byte {
	return m.msgChan
}
