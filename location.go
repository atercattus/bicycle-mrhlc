package main

import (
	"bytes"
	"log"
	"strconv"
)

type (
	Location struct {
		Id         int32
		Place      []byte
		CountryIdx int32
		City       []byte
		Distance   int32

		cache LocationsAvg
	}
)

func (l *Location) Reset() {
	l.Id = 0
	l.Place = l.Place[:0]
	l.CountryIdx = 0
	l.City = l.City[:0]
	l.Distance = 0
	l.cache.locations = l.cache.locations[:0]
}

func (l *Location) Parse(buf []byte) bool {
	changed := false

	err := ParseItem(buf, func(key, value []byte, valueType JSValueType) bool {
		changed = true // проверка, что не совсем пустой buf пришел

		if bytes.Equal(key, strId) {
			if valueType != jsValueTypeNumeric {
				return false
			} else if i64, ok := byteSliceToInt64(value); !ok {
				return false
			} else {
				l.Id = int32(i64)
			}
		} else if bytes.Equal(key, strPlace) {
			if valueType != jsValueTypeString {
				return false
			}
			l.Place = append(l.Place[0:0], value...)
		} else if bytes.Equal(key, strCountry) {
			if valueType != jsValueTypeString {
				return false
			}

			country := utf8Unescaped(value)
			idx, _ := indexCountry.Add(country)
			l.CountryIdx = int32(idx)
		} else if bytes.Equal(key, strCity) {
			if valueType != jsValueTypeString {
				return false
			}
			l.City = append(l.City[0:0], value...)
		} else if bytes.Equal(key, strDistance) {
			if valueType != jsValueTypeNumeric {
				return false
			} else if i64, ok := byteSliceToInt64(value); !ok {
				return false
			} else {
				l.Distance = int32(i64)
			}
		} // else { // неизвестный ключ

		return true
	})

	return (err == nil) && changed
}

func (l *Location) Serialize(buf []byte) []byte {
	country := indexCountry.GetByIdx(int(l.CountryIdx))

	buf = append(buf, `{"id":`...)
	buf = strconv.AppendInt(buf, int64(l.Id), 10)
	buf = append(buf, `,"place":"`...)
	buf = append(buf, l.Place...)
	buf = append(buf, `","country":"`...)
	buf = append(buf, country...)
	buf = append(buf, `","city":"`...)
	buf = append(buf, l.City...)
	buf = append(buf, `","distance":`...)
	buf = strconv.AppendInt(buf, int64(l.Distance), 10)
	buf = append(buf, '}')

	return buf
}

func (l *Location) CheckFields(update bool) bool {
	/*if utf8EscapedStringLen(l.Country) > 50 {
		return false
	} else if utf8EscapedStringLen(l.City) > 50 {
		return false
	}*/

	if !update && (len(l.City) == 0 || l.CountryIdx == 0 || l.Distance == 0 || len(l.Place) == 0) {
		// при создании должны передаваться все поля
		return false
	}

	if update && (l.Id != 0) {
		// id не может обновляться
		return false
	}

	return true
}

func (l *Location) Update(update *Location) bool {
	/*
		Если меняется Distance:
			- в UserVisits (Visit(Location.cache.visitId) => User(visit.User).cache.distance)
			    поменять поля distance, countryIdx
	*/

	placeChanged := (len(update.Place) != 0) && !bytes.Equal(l.Place, update.Place)
	if placeChanged {
		l.Place = append(l.Place[:0], update.Place...)
	}

	countryChanged := (update.CountryIdx != 0) && (l.CountryIdx != update.CountryIdx)
	if countryChanged {
		l.CountryIdx = update.CountryIdx
	}

	if len(update.City) != 0 {
		l.City = append(l.City[:0], update.City...)
	}

	distanceChanged := (update.Distance != 0) && (l.Distance != update.Distance)
	if distanceChanged {
		l.Distance = update.Distance
	}

	if placeChanged || distanceChanged || countryChanged {
		l.cacheUpdateDistanceAndCountryIdxAndPlace()
	}

	return true
}

func (l *Location) cacheUpdateDistanceAndCountryIdxAndPlace() {
	for _, la := range l.cache.locations {
		if visit := indexVisit.Get(la.visitId); visit == nil {
			log.Println(`WTF visit nil in location cache`, la.visitId)
		} else if user := indexUser.Get(visit.User); user == nil {
			log.Println(`WTF user nil in location cache`, la.visitId, visit.User)
		} else {
			for i, uv := range user.cache.visits {
				if uv.visitId != visit.Id {
					continue
				}

				if l.Distance != 0 {
					user.cache.visits[i].distance = l.Distance
				}
				if l.CountryIdx != 0 {
					user.cache.visits[i].countryIdx = l.CountryIdx
				}
				if len(l.Place) != 0 {
					user.cache.visits[i].place = l.Place
				}
			}
		}
	}
}
