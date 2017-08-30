package main

import (
	"sync"
)

const (
	usersPreallocCount = 1.2 * 1000 * 1000
)

type (
	IndexUser struct {
		users      []User
		usersExtra map[int32]*User
		rwLock     sync.RWMutex
	}
)

func MakeIndexUser() *IndexUser {
	return &IndexUser{
		users:      make([]User, usersPreallocCount),
		usersExtra: make(map[int32]*User),
	}
}

func (iu *IndexUser) Add(user *User) bool {
	var ok bool

	if user.Id < usersPreallocCount {
		iu.rwLock.RLock()
		if iu.users[user.Id].Id == user.Id {
			ok = true
		}
		iu.rwLock.RUnlock()
		if ok {
			return false
		}

		iu.rwLock.Lock()
		if iu.users[user.Id].Id == user.Id {
			iu.rwLock.Unlock()
			return false
		}

		iu.users[user.Id] = *user

		iu.rwLock.Unlock()

		return true
	}

	// id вне заранее выделенного диапазона. задействуем map

	iu.rwLock.RLock()
	_, ok = iu.usersExtra[user.Id]
	iu.rwLock.RUnlock()
	if ok {
		return false
	}

	iu.rwLock.Lock()
	if _, ok := iu.usersExtra[user.Id]; ok {
		iu.rwLock.Unlock()
		return false
	}

	iu.usersExtra[user.Id] = user

	iu.rwLock.Unlock()

	return true
}

func (iu *IndexUser) Update(id int32, update *User) bool {
	var (
		user *User
		ok   bool
	)

	iu.rwLock.RLock()
	if id < usersPreallocCount {
		user = &iu.users[id]
		ok = user.Id == id
	} else {
		user, ok = iu.usersExtra[id]
	}
	iu.rwLock.RUnlock()
	if !ok {
		return false
	}

	return user.Update(update)
}

func (iu *IndexUser) Get(id int32) *User {
	var (
		user *User
		ok   bool
	)

	iu.rwLock.RLock()
	if id < usersPreallocCount {
		user = &iu.users[id]
		ok = user.Id == id
	} else {
		user, ok = iu.usersExtra[id]
	}
	iu.rwLock.RUnlock()
	if !ok {
		return nil
	}
	return user
}
