package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strconv"
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

// FileExInfo file info
type FileExInfo struct {
	fd       *os.File
	Setting  *FileReader
	exitChan chan int
	offsize  int64
	IsEOF    bool
	Name     string `json:"Name"`
	Inode    uint64 `json:"Inode"`
	Device   uint64 `json:"Device"`
}

// GetFileExInfo get file's exinfo
func GetFileExInfo(info os.FileInfo) (uint64, uint64) {
	stat := info.Sys().(*syscall.Stat_t)

	return uint64(stat.Ino), uint64(stat.Dev)
}

// IsSame check file is same or not
func (fs FileExInfo) IsSame(fInfo FileExInfo) bool {
	return fs.Inode == fInfo.Inode && fs.Device == fInfo.Device
}

// GetHashString get file's hash from file's exinfo
func (fs FileExInfo) GetHashString() string {
	return fmt.Sprintf("%d:%d", fs.Inode, fs.Device)
}

// NewFileExInfo get file's exinfo from file name
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
	fInfo.exitChan = make(chan int)
	fInfo.offsize = 0
	fInfo.IsEOF = false
	return fInfo, err
}

// ReadLoop read task
func (fs *FileExInfo) ReadLoop() {
	if fs.offsize == 0 {
		if fs.Setting.ReadAll {
			fs.offsize, _ = fs.fd.Seek(0, io.SeekStart)
		} else {
			fs.offsize, _ = fs.fd.Seek(0, io.SeekEnd)
		}
	}
	reader := bufio.NewReader(fs.fd)
	for {
		select {
		case <-fs.exitChan:
			fs.offsize, _ = fs.fd.Seek(0, io.SeekCurrent)
			return
		default:
			fs.offsize, _ = fs.fd.Seek(0, io.SeekCurrent)
			line, err := reader.ReadBytes('\n')
			if err != nil && err != io.EOF {
				fmt.Println(err)
				if len(line) > 0 {
					fs.fd.Seek(fs.offsize, io.SeekStart)
				}
				time.Sleep(time.Second)
				break
			}
			if err == io.EOF {
				// check same name is rename or not
				fInfo, err := NewFileExInfo(fs.Name, fs.Setting)
				if err == nil {
					if !fs.IsSame(*fInfo) {
						// renamed
						fs.IsEOF = true
						fs.Setting.refreshChan <- 1
					} else {
						offset, _ := fInfo.fd.Seek(0, io.SeekEnd)
						if offset < fs.offsize {
							fs.offsize = 0
							fs.fd.Seek(0, io.SeekStart)
							reader = bufio.NewReader(fs.fd)
							break
						}
					}
					fInfo.Stop()
				}
				if len(line) == 0 {
					time.Sleep(time.Second)
					break
				}
				if line[len(line)-1] != byte('\n') {
					// reset readline
					fs.fd.Seek(fs.offsize, io.SeekStart)
					reader = bufio.NewReader(fs.fd)
					time.Sleep(time.Second)
					break
				}
			}
			if len(line) > 0 {
				logmsg := make(map[string][]byte)
				logmsg["msg"] = line
				fs.Setting.msgChan <- &logmsg
			}
		}
	}
}

// Stop all
func (fs FileExInfo) Stop() {
	close(fs.exitChan)
	fs.fd.Close()
}

// FileReader read file
type FileReader struct {
	sync.Mutex
	Files       map[string]*FileExInfo
	LastStats   map[string]int64
	msgChan     chan *map[string][]byte
	Name        string
	StatusDir   string
	refreshChan chan int
	FileList    string
	ReadAll     bool
	exitChan    chan int
}

// NewFileReader create FileReader
func NewFileReader(config map[string]string) (*FileReader, error) {
	m := &FileReader{}
	m.exitChan = make(chan int)
	m.refreshChan = make(chan int)
	m.msgChan = make(chan *map[string][]byte)
	m.Files = make(map[string]*FileExInfo)
	m.FileList = config["Files"]
	if len(m.FileList) == 0 {
		return m, fmt.Errorf("bad config")
	}
	m.ReadAll = false
	if config["ReadAll"] == "true" {
		m.ReadAll = true
	}
	m.Name = config["Name"]
	m.StatusDir = config["StatusDir"]
	if len(m.StatusDir) == 0 {
		m.StatusDir = "/tmp"
	}
	err := m.GetFiles()
	go func() {
		ticker := time.Tick(time.Minute)
		for {
			select {
			case <-ticker:
				m.ReadAll = true
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

// GetLastInfo get info from disk
func (m *FileReader) GetLastInfo() {
	statfile, err := os.Open(fmt.Sprintf("%s/.%slazystatus", m.StatusDir, m.Name))
	if err == nil {
		content, err := ioutil.ReadAll(statfile)
		if err == nil {
			lines := strings.Split(string(content), "\n")
			for _, line := range lines {
				items := strings.Split(line, " ")
				if len(items) == 2 {
					m.LastStats[items[0]], _ = strconv.ParseInt(items[1], 10, 64)
				}
			}
		}
	}
	statfile.Close()

}

// GetFiles get files
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
	exactFile, err := NewFileExInfo(m.FileList, m) // newfile
	if err == nil {
		fileMap[exactFile.GetHashString()] = exactFile.Name
		_, ok := m.Files[exactFile.GetHashString()]
		if ok { // ignore old config
			exactFile.fd.Close()
		} else {
			m.Files[exactFile.GetHashString()] = exactFile
			log.Println("start reading", exactFile.Name)
			if offsize, ok := m.LastStats[exactFile.GetHashString()]; ok {
				exactFile.offsize = offsize
				delete(m.LastStats, exactFile.GetHashString())
			}
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
				if offsize, ok := m.LastStats[fInfo.GetHashString()]; ok {
					fInfo.offsize = offsize
					delete(m.LastStats, exactFile.GetHashString())
				}
				go fInfo.ReadLoop()
			}
		}
	}
	for _, v := range m.Files {
		if _, ok := fileMap[v.GetHashString()]; !ok {
			if v.IsEOF {
				m.Files[v.GetHashString()].Stop()
				log.Println("Close File", m.Files[v.GetHashString()].Name)
				delete(m.Files, v.GetHashString())
			}
		}
	}
	m.Unlock()
	return nil
}

// Stop stop tasks
func (m *FileReader) Stop() {
	close(m.exitChan)
	statfile, err := os.OpenFile(fmt.Sprintf("%s/.%slazystatus", m.StatusDir, m.Name), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println("failed to open log file", err)
	}
	defer statfile.Close()
	m.Lock()
	for _, file := range m.Files {
		file.Stop()
		fmt.Fprintf(statfile, "%s %d\n", file.GetHashString(), file.offsize)
	}
	m.Unlock()
}

// GetMsgChan return msgChan
func (m *FileReader) GetMsgChan() chan *map[string][]byte {
	return m.msgChan
}
