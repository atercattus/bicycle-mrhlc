package main

import (
	"bytes"
	"sync"
)

type (
	IndexCountry struct {
		names  [][]byte
		rwLock sync.RWMutex
	}
)

func MakeIndexCountry() *IndexCountry {
	index := &IndexCountry{}
	index.names = make([][]byte, 0, 300)
	index.names = append(index.names, nil) // резервирую 0 индекс, чтобы не выдавался
	return index
}

func (ic *IndexCountry) Add(name []byte) (int, bool) {
	ic.rwLock.Lock()
	for idx, n := range ic.names {
		if bytes.Equal(n, name) {
			ic.rwLock.Unlock()
			return idx, false
		}
	}
	ic.names = append(ic.names, name)
	idx := len(ic.names) - 1
	ic.rwLock.Unlock()
	return idx, true
}

func (ic *IndexCountry) Find(name []byte) (idx int) { // 0 - если не найден
	ic.rwLock.RLock()
	for i, n := range ic.names {
		if bytes.Equal(n, name) {
			idx = i
			break
		}
	}
	ic.rwLock.RUnlock()
	return
}

func (ic *IndexCountry) GetByIdx(idx int) (name []byte) {
	ic.rwLock.RLock()
	if (idx >= 0) && (idx < len(ic.names)) {
		name = ic.names[idx]
	}
	ic.rwLock.RUnlock()
	return
}
