package main

import (
	"sync"
)

type Worker interface {
	Stop()
	DetailInfo() []byte
	GetName() string
	IsGoodConfig(config []byte) bool
}

type TaskPool struct {
	sync.Mutex
	workers map[string]Worker
}

func (t *TaskPool) Join(w Worker) {
	t.Lock()
	if _, ok := t.workers[w.GetName()]; !ok {
		t.workers[w.GetName()] = w
	}
	t.Unlock()
}

func (t *TaskPool) IsStarted(id string) bool {
	t.Lock()
	if _, ok := t.workers[id]; !ok {
		t.Unlock()
		return false
	}
	t.Unlock()
	return true
}
func (t *TaskPool) Cleanup(list map[string]string) {
	t.Lock()
	for k, v := range t.workers {
		config, ok := list[k]
		if !ok {
			v.Stop()
			delete(t.workers, k)
		}
		if config != string(v.DetailInfo()) {
			if v.IsGoodConfig([]byte(list[k])) {
				v.Stop()
				delete(t.workers, k)
			}
		}
	}
	t.Unlock()
}
func NewTaskPool() *TaskPool {
	return &TaskPool{workers: make(map[string]Worker)}
}

func (t *TaskPool) Stop() {
	t.Lock()
	for _, w := range t.workers {
		w.Stop()
	}
	t.Unlock()
}
