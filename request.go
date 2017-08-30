package main

import (
	"bytes"
	"sync"
)

type (
	RequestParams struct {
		isGET  bool
		isNew  bool
		id     int32
		entity []byte
		action []byte

		fromDate   int32 // visited_at > fromDate
		toDate     int32 // visited_at < toDate
		countryIdx int   // название страны (индекс в indexCountry), в которой находятся интересующие достопримечательности
		toDistance int32 // возвращать только те места, у которых расстояние от города меньше этого параметра
		fromAge    int32 // учитывать только путешественников, у которых возраст (в годах) (считается от текущего timestamp) больше этого параметра
		toAge      int32 // как предыдущее, но наоборот
		gender     byte  // учитывать оценки только мужчин или женщин
	}
)

var (
	requestParamsPool = sync.Pool{
		New: func() interface{} {
			return &RequestParams{}
		},
	}
)

func parseArgs(ctx *RequestCtx, args []byte, params *RequestParams) bool {
	if len(args) == 0 {
		return true
	}

	var arg, val []byte

	for len(args) > 0 {
		pEq := bytes.IndexByte(args, '=')
		pAmp := bytes.IndexByte(args, '&')
		if pEq == -1 {
			return false
		} else if pAmp > -1 && pAmp < pEq {
			return false
		}

		arg = args[:pEq]
		if pAmp == -1 {
			val = args[pEq+1:]
			args = nil
		} else {
			val = args[pEq+1 : pAmp]
			args = args[pAmp+1:]
		}

		if len(val) == 0 {
			return false
		}

		if bytes.Equal(arg, strFromDate) {
			if i64, ok := byteSliceToInt64(val); !ok {
				return false
			} else {
				params.fromDate = int32(i64)
			}
		} else if bytes.Equal(arg, strToDate) {
			if i64, ok := byteSliceToInt64(val); !ok {
				return false
			} else {
				params.toDate = int32(i64)
			}
		} else if bytes.Equal(arg, strCountry) {
			country := urlDecode(ctx.UserBuf[:0], val)
			params.countryIdx = indexCountry.Find(country)
		} else if bytes.Equal(arg, strToDistance) {
			if i64, ok := byteSliceToInt64(val); !ok {
				return false
			} else {
				params.toDistance = int32(i64)
			}
		} else if bytes.Equal(arg, strFromAge) {
			if i64, ok := byteSliceToInt64(val); !ok {
				return false
			} else {
				params.fromAge = int32(i64)
			}
		} else if bytes.Equal(arg, strToAge) {
			if i64, ok := byteSliceToInt64(val); !ok {
				return false
			} else {
				params.toAge = int32(i64)
			}
		} else if bytes.Equal(arg, strGender) {
			if (len(val) != 1) || ((val[0] != 'm') && (val[0] != 'f')) {
				return false
			}
			params.gender = val[0]
		}
	}

	return true
}

func parseRequest(ctx *RequestCtx, params *RequestParams) bool {
	// method
	params.isGET = ctx.Method == MethodGET

	uri := ctx.Path[1:] // убираю начальный /

	var args []byte

	// path?args
	if idx := bytes.IndexByte(uri, '?'); idx > 0 {
		args = uri[idx+1:]
		uri = uri[:idx]
	}

	// entity
	if idx := bytes.IndexByte(uri, '/'); idx > 0 {
		params.entity = uri[0:idx]
		uri = uri[idx+1:]
	} else if idx == -1 {
		params.entity = uri
		uri = nil
	} else {
		return false
	}

	params.isNew = false
	params.action = nil
	params.id = 0
	params.fromDate = 0
	params.toDate = 0
	params.countryIdx = 0
	params.toDistance = 0
	params.fromAge = 0
	params.toAge = 0
	params.gender = 0

	if len(uri) == 0 {
		return true
	}

	// id
	if bytes.Equal(uri, strNew) {
		// /<entity>/new
		params.isNew = true
		uri = nil
	} else if idx := bytes.IndexByte(uri, '/'); idx == 0 {
		return false
	} else {
		// /<entity>/<id>
		tail := uri
		to := idx
		if to == -1 {
			to = len(uri)
			tail = nil
		} else {
			tail = uri[to+1:]
		}

		if i64, ok := byteSliceToInt64(uri[0:to]); ok {
			params.id = int32(i64)
		} // else  // могут быть всякие "/users/bad". это корректный запрос, просто 404

		uri = tail
	}

	// action
	if len(uri) > 0 {
		// /users/<id>/visits
		params.action = uri
	}

	return parseArgs(ctx, args, params)
}
