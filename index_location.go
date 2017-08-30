package main

import (
	"sync"
)

const (
	locationsPreallocCount = 800 * 1000
)

type (
	IndexLocation struct {
		locations      []Location
		locationsExtra map[int32]*Location // для тех, чей id не вписывается в норму
		rwLock         sync.RWMutex
	}
)

func MakeIndexLocation() *IndexLocation {
	return &IndexLocation{
		locations:      make([]Location, locationsPreallocCount),
		locationsExtra: make(map[int32]*Location),
	}
}

func (il *IndexLocation) Add(location *Location) bool {
	var ok bool

	if location.Id < locationsPreallocCount {
		il.rwLock.RLock()
		if il.locations[location.Id].Id == location.Id {
			ok = true
		}
		il.rwLock.RUnlock()
		if ok {
			return false
		}

		il.rwLock.Lock()
		if il.locations[location.Id].Id == location.Id {
			il.rwLock.Unlock()
			return false
		}

		il.locations[location.Id] = *location

		il.rwLock.Unlock()

		return true
	}

	// id вне заранее выделенного диапазона. задействуем map

	il.rwLock.RLock()
	_, ok = il.locationsExtra[location.Id]
	il.rwLock.RUnlock()
	if ok {
		return false
	}

	il.rwLock.Lock()
	if _, ok = il.locationsExtra[location.Id]; ok {
		il.rwLock.Unlock()
		return false
	}

	il.locationsExtra[location.Id] = location

	il.rwLock.Unlock()

	return true
}

func (il *IndexLocation) Update(id int32, update *Location) bool {
	var (
		location *Location
		ok       bool
	)

	il.rwLock.RLock()
	if id < locationsPreallocCount {
		location = &il.locations[id]
		ok = location.Id == id
	} else {
		location, ok = il.locationsExtra[id]
	}
	il.rwLock.RUnlock()
	if !ok {
		return false
	}

	return location.Update(update)
}

func (il *IndexLocation) Get(id int32) *Location {
	var (
		location *Location
		ok       bool
	)

	il.rwLock.RLock()
	if id < locationsPreallocCount {
		location = &il.locations[id]
		ok = location.Id == id
	} else {
		location, ok = il.locationsExtra[id]
	}
	il.rwLock.RUnlock()
	if !ok {
		return nil
	}
	return location
}
