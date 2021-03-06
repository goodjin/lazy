package main

import (
	"sync"
)

// Worker interface for worker
type Worker interface {
	Stop()
	DetailInfo() []byte
	GetName() string
	IsGoodConfig(config []byte) bool
}

// TaskPool worker list
type TaskPool struct {
	sync.Mutex
	workers map[string]Worker
}

// Join add worker to taskpool
func (t *TaskPool) Join(w Worker) {
	t.Lock()
	if _, ok := t.workers[w.GetName()]; !ok {
		t.workers[w.GetName()] = w
	}
	t.Unlock()
}

// IsStarted check task started or not
func (t *TaskPool) IsStarted(id string) bool {
	t.Lock()
	if _, ok := t.workers[id]; !ok {
		t.Unlock()
		return false
	}
	t.Unlock()
	return true
}

// Cleanup remove all
func (t *TaskPool) Cleanup(list map[string]string) {
	t.Lock()
	for k, v := range t.workers {
		config, ok := list[k]
		if !ok {
			v.Stop()
			delete(t.workers, k)
		}
		if config != string(v.DetailInfo()) {
			if v.IsGoodConfig([]byte(config)) {
				v.Stop()
				delete(t.workers, k)
			}
		}
	}
	t.Unlock()
}

// NewTaskPool create TaskPool
func NewTaskPool() *TaskPool {
	return &TaskPool{workers: make(map[string]Worker)}
}

// Stop close all
func (t *TaskPool) Stop() {
	t.Lock()
	for _, w := range t.workers {
		w.Stop()
	}
	t.Unlock()
}
