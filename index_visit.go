package main

import (
	"sync"
)

const (
	visitsPreallocCount = 12 * 1000 * 1000
)

type (
	IndexVisit struct {
		visits      []Visit
		visitsExtra map[int32]*Visit
		rwLock      sync.RWMutex
	}
)

func MakeIndexVisit() *IndexVisit {
	return &IndexVisit{
		visits:      make([]Visit, visitsPreallocCount),
		visitsExtra: make(map[int32]*Visit),
	}
}

func (iv *IndexVisit) Add(visit *Visit) bool {
	var ok bool

	if visit.Id < visitsPreallocCount {
		iv.rwLock.RLock()
		if iv.visits[visit.Id].Id == visit.Id {
			ok = true
		}
		iv.rwLock.RUnlock()
		if ok {
			return false
		}

		iv.rwLock.Lock()
		if iv.visits[visit.Id].Id == visit.Id {
			iv.rwLock.Unlock()
			return false
		}

		iv.visits[visit.Id] = *visit

		iv.rwLock.Unlock()

		return true
	}

	// id вне заранее выделенного диапазона. задействуем map

	iv.rwLock.RLock()
	_, ok = iv.visitsExtra[visit.Id]
	iv.rwLock.RUnlock()
	if ok {
		return false
	}

	iv.rwLock.Lock()
	if _, ok := iv.visitsExtra[visit.Id]; ok {
		iv.rwLock.Unlock()
		return false
	}

	iv.visitsExtra[visit.Id] = visit

	iv.rwLock.Unlock()

	return true
}

func (iv *IndexVisit) Update(id int32, update *Visit) bool {
	var (
		visit *Visit
		ok    bool
	)

	iv.rwLock.RLock()
	if id < visitsPreallocCount {
		visit = &iv.visits[id]
		ok = visit.Id == id
	} else {
		visit, ok = iv.visitsExtra[id]
	}
	iv.rwLock.RUnlock()
	if !ok {
		return false
	}

	return visit.Update(update)
}

func (iv *IndexVisit) Get(id int32) *Visit {
	var (
		visit *Visit
		ok    bool
	)

	iv.rwLock.RLock()
	if id < visitsPreallocCount {
		visit = &iv.visits[id]
		ok = visit.Id == id
	} else {
		visit, ok = iv.visitsExtra[id]
	}
	iv.rwLock.RUnlock()
	if !ok {
		return nil
	}
	return visit
}
