package main

import (
	"sync"
)

type Worker interface {
	Stop()
	DetailInfo() []byte
	GetID() string
}

type TaskPool struct {
	sync.Mutex
	workers map[string]Worker
}

func (t *TaskPool) Join(w Worker) {
	t.Lock()
	if _, ok := t.workers[w.GetID()]; !ok {
		t.workers[w.GetID()] = w
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
		config := list[k]
		if string(v.DetailInfo()) == config {
			continue
		}
		v.Stop()
		delete(t.workers, k)
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
