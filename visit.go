package main

import (
	"bytes"
	"log"
	"strconv"
)

const (
	minMarkValue = 0
	maxMarkValue = 5
)

type (
	Visit struct {
		Id         int32
		Location   int32
		User       int32
		VisitedAt  int32
		Mark       uint8
		markSetted bool
	}
)

func (v *Visit) Parse(buf []byte) bool {
	changed := false

	err := ParseItem(buf, func(key, value []byte, valueType JSValueType) bool {
		changed = true // проверка, что не совсем пустой buf пришел

		if bytes.Equal(key, strId) {
			if valueType != jsValueTypeNumeric {
				return false
			} else if i64, ok := byteSliceToInt64(value); !ok {
				return false
			} else {
				v.Id = int32(i64)
			}
		} else if bytes.Equal(key, strLocation) {
			if valueType != jsValueTypeNumeric {
				return false
			} else if i64, ok := byteSliceToInt64(value); !ok {
				return false
			} else {
				v.Location = int32(i64)
			}
		} else if bytes.Equal(key, strUser) {
			if valueType != jsValueTypeNumeric {
				return false
			} else if i64, ok := byteSliceToInt64(value); !ok {
				return false
			} else {
				v.User = int32(i64)
			}
		} else if bytes.Equal(key, strVisitedAt) {
			if valueType != jsValueTypeNumeric {
				return false
			} else if i64, ok := byteSliceToInt64(value); !ok {
				return false
			} else {
				v.VisitedAt = int32(i64)
			}
		} else if bytes.Equal(key, strMark) {
			if valueType != jsValueTypeNumeric {
				return false
			} else if mark, ok := byteSliceToInt64(value); !ok || (mark < minMarkValue) || (mark > maxMarkValue) {
				return false
			} else {
				v.Mark = uint8(mark)
				v.markSetted = true
			}
		} // else { // неизвестный ключ

		return true
	})

	return (err == nil) && changed
}

func (v *Visit) Serialize(buf []byte) []byte {
	buf = append(buf, `{"id":`...)
	buf = strconv.AppendInt(buf, int64(v.Id), 10)
	buf = append(buf, `,"location":`...)
	buf = strconv.AppendInt(buf, int64(v.Location), 10)
	buf = append(buf, `,"user":`...)
	buf = strconv.AppendInt(buf, int64(v.User), 10)
	buf = append(buf, `,"visited_at":`...)
	buf = strconv.AppendInt(buf, int64(v.VisitedAt), 10)
	buf = append(buf, `,"mark":`...)
	buf = strconv.AppendInt(buf, int64(v.Mark), 10)
	buf = append(buf, '}')

	return buf
}

func (v *Visit) CheckFields(update bool) bool {
	// ToDo: location - id
	// ToDo: user - id
	if v.Mark > maxMarkValue {
		return false
	}

	if !update && (!v.markSetted || v.VisitedAt == 0 || v.User == 0 || v.Location == 0) {
		// при создании должны передаваться все поля
		return false
	}

	if update && (v.Id != 0) {
		// id не может обновляться
		return false
	}

	return true
}

func (v *Visit) Update(update *Visit) bool {
	/*
		Если меняется Location:
			- удалить (по LocationAvg.visitId) из старого Location.cache и добавить в новый
			- в UserVisits обновить поля distance, countryIdx
		Если меняется User:
			- удалить (по UserVisit.visitId) из старого User.cache и добавить в новый
			- в LocationsAvg обновить поля birthdate и gender
		Если меняется VisitedAt:
			- в LocationsAvg обновить visitedAt
			- в UserVisits обновить visitedAt
		Если меняется Mark:
			- в LocationsAvg обновить mark
	*/

	if update.Location != 0 && (v.Location != update.Location) {
		old := v.Location
		v.Location = update.Location
		v.cacheUpdateLocation(old)
	}

	if update.User != 0 && (v.User != update.User) {
		old := v.User
		v.User = update.User
		v.cacheUpdateUser(old)
	}

	if update.VisitedAt != 0 && (v.VisitedAt != update.VisitedAt) {
		v.VisitedAt = update.VisitedAt
		v.cacheUpdateVisitedAt()
	}

	if update.markSetted && (v.Mark != update.Mark) {
		v.Mark = update.Mark
		v.cacheUpdateMark()
	}

	return true
}

func (v *Visit) cacheUpdateLocation(oldLocationId int32) {
	if location := indexLocation.Get(v.Location); location == nil {
		log.Println(`WTF location nil in visit`, v.Location)
	} else if user := indexUser.Get(v.User); user == nil {
		log.Println(`WTF user nil in visit`, v.User)
	} else if locationOld := indexLocation.Get(oldLocationId); locationOld == nil {
		log.Println(`WTF location nil in visit`, oldLocationId)
	} else {
		locationOld.cache.MoveByVisitId(location, v.Id)

		for i, uv := range user.cache.visits {
			if uv.visitId == v.Id {
				user.cache.visits[i].place = location.Place
				user.cache.visits[i].distance = location.Distance
				user.cache.visits[i].countryIdx = location.CountryIdx
			}
		}
	}
}

func (v *Visit) cacheUpdateUser(oldUserId int32) {
	if user := indexUser.Get(v.User); user == nil {
		log.Println(`WTF user nil in visit`, v.User)
	} else if location := indexLocation.Get(v.Location); location == nil {
		log.Println(`WTF location nil in visit`, v.Location)
	} else if userOld := indexUser.Get(oldUserId); userOld == nil {
		log.Println(`WTF user nil in visit`, oldUserId)
	} else {
		userOld.cache.MoveByVisitId(user, v.Id)

		for i, la := range location.cache.locations {
			if la.visitId == v.Id {
				location.cache.locations[i].birthdate = user.BirthDate
				location.cache.locations[i].gender = user.Gender
			}
		}
	}
}

func (v *Visit) cacheUpdateVisitedAt() {
	if location := indexLocation.Get(v.Location); location == nil {
		log.Println(`WTF location nil in visit`, v.Location)
	} else {
		for i, la := range location.cache.locations {
			if la.visitId == v.Id {
				location.cache.locations[i].visitedAt = v.VisitedAt
			}
		}
	}

	if user := indexUser.Get(v.User); user == nil {
		log.Println(`WTF user nil in visit`, v.User)
	} else {
		user.cache.ChangeVisitedAtByVisitId(v.Id, v.VisitedAt)
	}
}

func (v *Visit) cacheUpdateMark() {
	if location := indexLocation.Get(v.Location); location == nil {
		log.Println(`WTF location nil in visit`, v.Location)
	} else {
		for i, la := range location.cache.locations {
			if la.visitId == v.Id {
				location.cache.locations[i].mark = v.Mark
			}
		}
	}

	if user := indexUser.Get(v.User); user == nil {
		log.Println(`WTF user nil in visit`, v.User)
	} else {
		for i, uv := range user.cache.visits {
			if uv.visitId == v.Id {
				user.cache.visits[i].markChar = v.Mark + '0'
			}
		}
	}
}
