package main

import (
	"strconv"
)

/*
/users/<user_id>/visits
отсортированная по возрастанию дат

Возможные GET-параметры:
	fromDate - посещения с visited_at > fromDate
	toDate - посещения с visited_at < toDate
	country - название страны, в которой находятся интересующие достопримечательности
	toDistance - возвращать только те места, у которых расстояние от города меньше этого параметра

Пример корректного ответа на запрос:
	{
		"mark": 2,
		"visited_at": 1223268286,
		"place": "Кольский полуостров"
	},
*/

type (
	UserVisits struct {
		visits []UserVisit
	}

	UserVisit struct {
		visitId         int32
		visitedAt       int32
		visitedAtStr    [12]byte
		visitedAtStrLen int32
		distance        int32
		countryIdx      int32
		markChar        byte // уже символом, не 0..5
		place           []byte
	}
)

func (uv *UserVisits) allocSpaceByVisitedAt(visitedAt int32) (idx int) {
	l := len(uv.visits)

	for idx = 0; idx < l; idx++ {
		if uv.visits[idx].visitedAt > visitedAt {
			break
		}
	}

	uv.visits = append(uv.visits, UserVisit{})

	if idx == l {
		// добавление в конец или в пустой список
		// мы уже выделили место на +1 элемент. больше ничего делать не надо
	} else {
		// добавление в середину или начало, нужно сдвигать
		copy(uv.visits[idx+1:], uv.visits[idx:])
	}

	return idx
}

func (uv *UserVisits) Add(location *Location, visit *Visit) bool {
	idx := uv.allocSpaceByVisitedAt(visit.VisitedAt)

	item := &uv.visits[idx]
	item.visitId = visit.Id
	item.visitedAt = visit.VisitedAt
	buf := strconv.AppendInt(item.visitedAtStr[:0], int64(visit.VisitedAt), 10)
	item.visitedAtStrLen = int32(len(buf))
	item.distance = location.Distance
	item.countryIdx = location.CountryIdx
	item.markChar = visit.Mark + '0'
	item.place = location.Place

	return true
}

func (uv *UserVisits) MoveByVisitId(target *User, visitId int32) bool {
	currentPos := -1
	for i, uvItem := range uv.visits {
		if uvItem.visitId == visitId {
			currentPos = i
			break
		}
	}
	if currentPos == -1 {
		return false
	}

	bak := uv.visits[currentPos]

	// удаляем из себя
	l := len(uv.visits)
	if currentPos < l-1 {
		copy(uv.visits[currentPos:], uv.visits[currentPos+1:])
	}
	uv.visits = uv.visits[:l-1]

	// добавляем в новый список
	idx := target.cache.allocSpaceByVisitedAt(bak.visitedAt)
	target.cache.visits[idx] = bak

	return true
}

func (uv *UserVisits) ChangeVisitedAtByVisitId(visitId int32, visitedAt int32) {
	visitPos := -1
	for i, v := range uv.visits {
		if v.visitId == visitId {
			// тут поменять visitedAt еще нельзя, чтобы не сломался поиск новой позиции
			visitPos = i
			break
		}
	}
	if visitPos < 0 {
		return
	}

	// ищем, с кем поменяться местами
	switchPos := 0
	l := len(uv.visits)
	for switchPos = 0; switchPos < l; switchPos++ {
		if uv.visits[switchPos].visitedAt > visitedAt {
			break
		}
	}

	uv.visits[visitPos].visitedAt = visitedAt
	buf := strconv.AppendInt(uv.visits[visitPos].visitedAtStr[:0], int64(visitedAt), 10)
	uv.visits[visitPos].visitedAtStrLen = int32(len(buf))

	// меняемся местами
	bak := uv.visits[visitPos]
	if switchPos > visitPos {
		cnt := switchPos - visitPos - 1
		copy(uv.visits[visitPos:visitPos+cnt], uv.visits[visitPos+1:])
		uv.visits[switchPos-1] = bak
	} else if switchPos < visitPos {
		cnt := visitPos - switchPos
		copy(uv.visits[switchPos+1:], uv.visits[switchPos:switchPos+cnt])
		uv.visits[switchPos] = bak
	}
}
