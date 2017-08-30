package main

import (
	"bytes"
	"log"
	"strconv"
)

type (
	User struct {
		Id        int32
		Email     []byte
		FirstName []byte
		LastName  []byte
		Gender    byte
		BirthDate int64

		birthdateSetted bool

		cache UserVisits
	}
)

func (u *User) Parse(buf []byte) bool {
	changed := false

	u.birthdateSetted = false

	err := ParseItem(buf, func(key, value []byte, valueType JSValueType) bool {
		changed = true // проверка, что не совсем пустой buf пришел

		if bytes.Equal(key, strId) {
			if valueType != jsValueTypeNumeric {
				return false
			} else if i64, ok := byteSliceToInt64(value); !ok {
				return false
			} else {
				u.Id = int32(i64)
			}
		} else if bytes.Equal(key, strEmail) {
			if valueType != jsValueTypeString {
				return false
			}
			u.Email = append(u.Email[0:0], value...)
		} else if bytes.Equal(key, strFirstName) {
			if valueType != jsValueTypeString {
				return false
			}
			u.FirstName = append(u.FirstName[0:0], value...)
		} else if bytes.Equal(key, strLastName) {
			if valueType != jsValueTypeString {
				return false
			}
			u.LastName = append(u.LastName[0:0], value...)
		} else if bytes.Equal(key, strGender) {
			if (valueType != jsValueTypeString) || (len(value) != 1) {
				return false
			}
			u.Gender = value[0]
		} else if bytes.Equal(key, strBirthdate) {
			if valueType != jsValueTypeNumeric {
				return false
			} else if i64, ok := byteSliceToInt64(value); !ok {
				return false
			} else {
				u.BirthDate = i64
				u.birthdateSetted = true
			}
		} // else { // неизвестный ключ

		return true
	})

	return (err == nil) && changed
}

func (u *User) Serialize(buf []byte) []byte {
	buf = append(buf, `{"id":`...)
	buf = strconv.AppendInt(buf, int64(u.Id), 10)
	buf = append(buf, `,"email":"`...)
	buf = append(buf, u.Email...)
	buf = append(buf, `","first_name":"`...)
	buf = append(buf, u.FirstName...)
	buf = append(buf, `","last_name":"`...)
	buf = append(buf, u.LastName...)
	buf = append(buf, `","gender":"`...)
	buf = append(buf, u.Gender)
	buf = append(buf, `","birth_date":`...)
	buf = strconv.AppendInt(buf, int64(u.BirthDate), 10)
	buf = append(buf, '}')

	return buf
}

func (u *User) CheckFields(update bool) bool {
	/*if utf8EscapedStringLen(u.Email) > 100 {
		return false
	} else if utf8EscapedStringLen(u.FirstName) > 50 {
		return false
	} else if utf8EscapedStringLen(u.LastName) > 50 {
		return false
	} else*/
	if u.Gender != 0 && u.Gender != 'm' && u.Gender != 'f' {
		return false
	}

	if !update && (len(u.Email) == 0 || len(u.FirstName) == 0 || len(u.LastName) == 0 || u.Gender == 0 || !u.birthdateSetted) {
		// при создании должны передаваться все поля
		return false
	}

	if update && (u.Id != 0) {
		// id не может обновляться
		return false
	}

	return true
}

func (u *User) Update(update *User) bool {
	/*
		Если меняется Gender:
			- в LocationsAvg обновить поле gender для: Visit(User.cache.visitId) => Location(Visit.Location).cache.gender
		Если меняется birthdate:
			- в LocationsAvg обновить поле birthdate для: Visit(User.cache.visitId) => Location(Visit.Location).cache.birthdate
	*/

	if len(update.Email) != 0 {
		u.Email = update.Email
	}

	if len(update.FirstName) != 0 {
		u.FirstName = update.FirstName
	}

	if len(update.LastName) != 0 {
		u.LastName = update.LastName
	}

	if update.Gender != 0 && (u.Gender != update.Gender) {
		u.Gender = update.Gender
		u.cacheUpdateGender()
	}

	if update.birthdateSetted && (u.BirthDate != update.BirthDate) {
		u.BirthDate = update.BirthDate
		u.cacheUpdateBirthdate()
	}

	return true
}

func (u *User) cacheUpdateBirthdate() {
	for _, uv := range u.cache.visits {
		if visit := indexVisit.Get(uv.visitId); visit == nil {
			log.Println(`WTF visit nil in user cache`, uv.visitId)
		} else if location := indexLocation.Get(visit.Location); location == nil {
			log.Println(`WTF location nil in user cache`, visit.Location)
		} else {
			for i, la := range location.cache.locations {
				if la.visitId == visit.Id {
					location.cache.locations[i].birthdate = u.BirthDate
				}
			}
		}
	}
}

func (u *User) cacheUpdateGender() {
	for _, uv := range u.cache.visits {
		if visit := indexVisit.Get(uv.visitId); visit == nil {
			log.Println(`WTF visit nil in user cache`, uv.visitId)
		} else if location := indexLocation.Get(visit.Location); location == nil {
			log.Println(`WTF location nil in user cache`, visit.Location)
		} else {
			for i, la := range location.cache.locations {
				if la.visitId == visit.Id {
					location.cache.locations[i].gender = u.Gender
				}
			}
		}
	}
}
