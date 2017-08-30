package main

/*
/locations/<location_id>/avg

Возможные GET-параметры:
	fromDate - учитывать оценки только с visited_at > fromDate
	toDate - учитывать оценки только с visited_at < toDate
	fromAge - учитывать только путешественников, у которых возраст (считается от текущего timestamp) больше этого параметра
	toAge - как предыдущее, но наоборот
	gender - учитывать оценки только мужчин или женщин
*/

type (
	LocationsAvg struct {
		locations []LocationAvg
	}

	LocationAvg struct {
		visitId   int32
		visitedAt int32
		birthdate int64
		gender    byte

		mark uint8
	}
)

func (la *LocationsAvg) Add(location *Location, visit *Visit, user *User) bool {
	la.locations = append(la.locations, LocationAvg{
		visitId:   visit.Id,
		visitedAt: visit.VisitedAt,
		birthdate: user.BirthDate,
		gender:    user.Gender,
		mark:      visit.Mark,
	})
	return true
}

func (la *LocationsAvg) MoveByVisitId(target *Location, visitId int32) bool {
	currentPos := -1
	for i, laItem := range la.locations {
		if laItem.visitId == visitId {
			currentPos = i
			break
		}
	}
	if currentPos == -1 {
		return false
	}

	bak := la.locations[currentPos]

	lastIdx := len(la.locations) - 1
	if currentPos < lastIdx {
		la.locations[currentPos] = la.locations[lastIdx]
	}
	la.locations = la.locations[:lastIdx]

	target.cache.locations = append(target.cache.locations, bak)

	return true
}
