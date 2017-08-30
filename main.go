package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	ErrWrongData = errors.New(`Wrong data`)
)

var (
	indexUser     = MakeIndexUser()
	indexLocation = MakeIndexLocation()
	indexVisit    = MakeIndexVisit()

	indexCountry = MakeIndexCountry()
)

var (
	BuildInfo string = ``

	argv struct {
		port    uint
		help    bool
		pprof   bool
		zipPath string
	}

	dictStatistics struct {
		Users         int64 `json:"users"`
		Locations     int64 `json:"locations"`
		Visits        int64 `json:"visits"`
		LocationMaxId int32 `json:"location_max_id"`
		UserMaxId     int32 `json:"user_max_id"`
		VisitMaxId    int32 `json:"visit_max_id"`
		Elapsed       int64 `json:"elapsed"`
	}

	// текущее время (или реальное, или полученное из архива с данными)
	timeNow time.Time

	currentPhase int

	buf bytes.Buffer

	queries, prevQPS int64

	poolLocation = sync.Pool{
		New: func() interface{} {
			return &Location{}
		},
	}
)

func init() {
	flag.UintVar(&argv.port, `port`, 80, `port to listen`)
	flag.BoolVar(&argv.help, `h`, false, `show this help`)
	flag.BoolVar(&argv.pprof, `pprof`, false, `enable pprof`)
	flag.StringVar(&argv.zipPath, `zip`, `/tmp/data/data.zip`, `path to zip file`)
	flag.Parse()
}

func main() {
	if argv.help {
		fmt.Printf("Builded from %s\n", BuildInfo)
		flag.Usage()
		return
	}

	log.Printf("Started on %d CPUs\n", runtime.NumCPU())

	srv := HTTPServer{
		Handler: requestHandler,
	}

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	debug.SetGCPercent(100)

	determineCurrentTime()

	log.Println(`Load DB...`)

	mt := time.Now().UnixNano()
	if err := loadDB(); err != nil {
		log.Fatalf(`load DB fail: %s`, err)
	}
	dictStatistics.Elapsed = (time.Now().UnixNano() - mt) / int64(time.Millisecond)

	buf.Reset()
	json.NewEncoder(&buf).Encode(dictStatistics)
	log.Println(`DB loaded stats:`, string(bytes.TrimSpace(buf.Bytes())))

	if argv.pprof {
		if fd, err := os.Create(`pprof.cpu`); err == nil {
			pprof.StartCPUProfile(fd)
			defer func() {
				pprof.StopCPUProfile()
				fd.Close()
			}()
		}

		defer func() {
			if fd, err := os.Create(`pprof.mem`); err == nil {
				pprof.WriteHeapProfile(fd)
				fd.Close()
			}
		}()
	}

	warming()

	go func() {
		for {
			time.Sleep(1 * time.Second)
			qps := atomic.SwapInt64(&queries, 0)
			endPhase := (qps == 0) && (prevQPS != 0)
			newPhase := (qps != 0) && (prevQPS == 0)
			prevQPS = qps

			if endPhase {
				if currentPhase >= 3 {
					gcFrom := time.Now()
					runtime.GC()
					gcElapsed := time.Now().Sub(gcFrom)

					log.Printf("Phase ended. Goroutines: %d GC elapsed (ms): %d RSS: %dMB GC pauses (ms): %s\n",
						runtime.NumGoroutine(),
						int64(gcElapsed/time.Millisecond),
						getRSSMemory()/1024/1024,
						getGCStats(),
					)
				} else {
					log.Printf("Phase ended. RSS: %dMB\n",
						getRSSMemory()/1024/1024,
					)
				}
			} else if newPhase {
				currentPhase++
				log.Println(`Phase`, currentPhase, `started`)
			}
		}
	}()

	go func() {
		if err := srv.ListenAndServe(int(argv.port)); err != nil {
			log.Fatalf(`ListenAndServe fail: %s`, err)
		}
	}()

	runtime.GC()
	debug.SetGCPercent(-1)

	mlockallErr := syscall.Mlockall(syscall.MCL_CURRENT)

	log.Printf("Ready for work. Build: %s. RSS:% dMB. GC pauses before (ms): %s. GC disabled. mlockall: %s\n",
		BuildInfo,
		getRSSMemory()/1024/1024,
		getGCStats(),
		mlockallErr,
	)

	ch := make(chan os.Signal, 10)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch

	log.Printf("Bye. RSS: %dMB GC pauses (ms): %s\n", getRSSMemory()/1024/1024, getGCStats())
}

func requestHandler(ctx *RequestCtx) {
	atomic.AddInt64(&queries, 1)

	req := requestParamsPool.Get().(*RequestParams)
	defer requestParamsPool.Put(req)

	if !parseRequest(ctx, req) {
		ctx.ResponseStatus = 400
		return
	} else if req.entity == nil {
		ctx.ResponseStatus = 400
		return
	}

	if !req.isGET && req.isNew {
		// POST /<entity>/new на создание
		reqNew(ctx, req)
	} else if req.id <= 0 {
		ctx.ResponseStatus = 404
		return
	} else if !req.isGET {
		// POST /<entity>/<id> на обновление
		reqUpdate(ctx, req)
	} else if req.action == nil {
		// GET /<entity>/<id> для получения данных о сущности
		reqGet(ctx, req)
	} else if bytes.Equal(req.entity, strUsers) && bytes.Equal(req.action, strVisits) {
		// GET /users/<id>/visits для получения списка посещений пользователем
		reqUserVisits(ctx, req)
	} else if bytes.Equal(req.entity, strLocations) && bytes.Equal(req.action, strAvg) {
		// GET /locations/<id>/avg для получения средней оценки достопримечательности
		reqLocationAvg(ctx, req)
	} else {
		ctx.ResponseStatus = 400
		return
	}
}

func reqGet(ctx *RequestCtx, req *RequestParams) {
	// GET /<entity>/<id> для получения данных о сущности

	if bytes.Equal(req.entity, strUsers) {
		if user := indexUser.Get(req.id); user == nil {
			ctx.ResponseStatus = 404
		} else {
			ctx.ResponseBody = user.Serialize(ctx.UserBuf[:0])
		}
	} else if bytes.Equal(req.entity, strLocations) {
		if location := indexLocation.Get(req.id); location == nil {
			ctx.ResponseStatus = 404
		} else {
			ctx.ResponseBody = location.Serialize(ctx.UserBuf[:0])
		}
	} else if bytes.Equal(req.entity, strVisits) {
		if visit := indexVisit.Get(req.id); visit == nil {
			ctx.ResponseStatus = 404
		} else {
			ctx.ResponseBody = visit.Serialize(ctx.UserBuf[:0])
		}
	} else {
		ctx.ResponseStatus = 400
	}
}

func reqUserVisits(ctx *RequestCtx, req *RequestParams) {
	// GET /users/<id>/visits для получения списка посещений пользователем

	user := indexUser.Get(req.id)

	if user == nil {
		ctx.ResponseStatus = 404
		return
	}

	fromDate := req.fromDate
	toDate := req.toDate
	countryIdx := int32(req.countryIdx)
	toDistance := req.toDistance

	buf := ctx.UserBuf[:0]

	bufWithData := false

	buf = append(buf, `{"visits":[`...)

	for _, cacheItem := range user.cache.visits {
		if (fromDate > 0) && (cacheItem.visitedAt <= fromDate) {
			continue
		} else if (toDate > 0) && (cacheItem.visitedAt >= toDate) {
			continue
		} else if (toDistance > 0) && (cacheItem.distance >= toDistance) {
			continue
		} else if (countryIdx > 0) && (cacheItem.countryIdx != countryIdx) {
			continue
		}

		bufWithData = true

		buf = append(buf, `{"mark":`...)
		buf = append(buf, cacheItem.markChar)
		buf = append(buf, `,"visited_at":`...)
		buf = append(buf, cacheItem.visitedAtStr[:cacheItem.visitedAtStrLen]...)
		buf = append(buf, `,"place":"`...)
		buf = append(buf, cacheItem.place...)
		buf = append(buf, `"},`...)
	}

	if bufWithData {
		buf = buf[:len(buf)-1] // убираю последнюю запятую
	}

	buf = append(buf, `]}`...)

	ctx.ResponseBody = buf
}

func reqLocationAvg(ctx *RequestCtx, req *RequestParams) {
	// GET /locations/<id>/avg для получения средней оценки достопримечательности

	location := indexLocation.Get(req.id)

	if location == nil {
		ctx.ResponseStatus = 404
		return
	}

	var (
		markCnt, markSum int64
		avg              float64
	)

	fromDate := req.fromDate
	toDate := req.toDate
	fromAge := req.fromAge
	toAge := req.toAge

	fromAgeTimestamp := ageToTimestamp(fromAge)
	toAgeTimestamp := ageToTimestamp(toAge)

	gender := req.gender

	for _, cacheItem := range location.cache.locations {
		if (fromDate > 0) && (cacheItem.visitedAt <= fromDate) {
			continue
		} else if (toDate > 0) && (cacheItem.visitedAt >= toDate) {
			continue
		} else if (fromAge > 0) && (cacheItem.birthdate > fromAgeTimestamp) {
			continue
		} else if (toAge > 0) && (cacheItem.birthdate < toAgeTimestamp) {
			continue
		} else if (gender != 0) && (gender != cacheItem.gender) {
			continue
		}

		markSum += int64(cacheItem.mark)
		markCnt++
	}

	if markCnt > 0 {
		avg = float64(markSum) / float64(markCnt)
	}
	avg += 1e-10 // +eps как костыль для округления

	buf := ctx.UserBuf[:0]

	buf = append(buf, `{"avg":`...)
	buf = strconv.AppendFloat(buf, avg, 'f', 5, 64)
	buf = append(buf, '}')

	ctx.ResponseBody = buf
}

func reqNew(ctx *RequestCtx, req *RequestParams) {
	// POST /<entity>/new на создание

	if bytes.Equal(req.entity, strUsers) {
		var user User
		if !user.Parse(ctx.Body) {
			ctx.ResponseStatus = 400
			return
		} else if !user.CheckFields(false) {
			ctx.ResponseStatus = 400
			return
		} else if !indexUser.Add(&user) {
			ctx.ResponseStatus = 400
			return
		}

	} else if bytes.Equal(req.entity, strLocations) {
		location := poolLocation.Get().(*Location)
		location.Reset()
		if !location.Parse(ctx.Body) {
			ctx.ResponseStatus = 400
			poolLocation.Put(location)
			return
		} else if !location.CheckFields(false) {
			ctx.ResponseStatus = 400
			poolLocation.Put(location)
			return
		} else if !indexLocation.Add(location) {
			ctx.ResponseStatus = 400
			poolLocation.Put(location)
			return
		}
		// если все хорошо, то не возвращаю location в пул

	} else if bytes.Equal(req.entity, strVisits) {
		var visit Visit
		if !visit.Parse(ctx.Body) {
			ctx.ResponseStatus = 400
			return
		} else if !visit.CheckFields(false) {
			ctx.ResponseStatus = 400
			return
		} else if !indexVisit.Add(&visit) {
			ctx.ResponseStatus = 400
			return
		} else if user := indexUser.Get(visit.User); user == nil {
			ctx.ResponseStatus = 404
			return
		} else if location := indexLocation.Get(visit.Location); location == nil {
			ctx.ResponseStatus = 404
			return
		} else {
			user.cache.Add(location, &visit)
			location.cache.Add(location, &visit, user)
		}

	} else {
		ctx.ResponseStatus = 400
		return
	}

	ctx.ResponseBody = emptyResponseBody
}

func reqUpdate(ctx *RequestCtx, req *RequestParams) {
	// POST /<entity>/<id> на обновление

	if bytes.Equal(req.entity, strUsers) {
		var user User

		if !user.Parse(ctx.Body) {
			ctx.ResponseStatus = 400
			return
		}

		if !user.CheckFields(true) {
			// 404 приоритетнее, чем 400
			if indexUser.Get(req.id) == nil {
				ctx.ResponseStatus = 404
			} else {
				ctx.ResponseStatus = 400
			}
			return
		} else if !indexUser.Update(req.id, &user) {
			ctx.ResponseStatus = 404
			return
		}

	} else if bytes.Equal(req.entity, strLocations) {
		location := poolLocation.Get().(*Location)
		location.Reset()

		if !location.Parse(ctx.Body) {
			ctx.ResponseStatus = 400
			poolLocation.Put(location)
			return
		}

		if !location.CheckFields(true) {
			// 404 приоритетнее, чем 400
			if indexLocation.Get(req.id) == nil {
				ctx.ResponseStatus = 404
			} else {
				ctx.ResponseStatus = 400
			}
			poolLocation.Put(location)
			return
		} else if !indexLocation.Update(req.id, location) {
			ctx.ResponseStatus = 404
			poolLocation.Put(location)
			return
		}
		poolLocation.Put(location)

	} else if bytes.Equal(req.entity, strVisits) {
		var visit Visit

		if !visit.Parse(ctx.Body) {
			ctx.ResponseStatus = 400
			return
		}

		if !visit.CheckFields(true) {
			// 404 приоритетнее, чем 400
			if indexVisit.Get(req.id) == nil {
				ctx.ResponseStatus = 404
			} else {
				ctx.ResponseStatus = 400
			}
			return
		} else if !indexVisit.Update(req.id, &visit) {
			ctx.ResponseStatus = 404
			return
		}

	} else {
		ctx.ResponseStatus = 400
		return
	}

	ctx.ResponseBody = emptyResponseBody
}

func loadDB() error {
	r, err := zip.OpenReader(argv.zipPath)
	if err != nil {
		return err
	}
	defer r.Close()

	// для корректной загрузки нужно обходить файлы в определенном порядке типов

	priority := []string{`locations_`, `users_`, `visits_`}

	queue := map[string][]*zip.File{}

	for _, prefix := range priority {
		queue[prefix] = []*zip.File{}
	}

	for _, f := range r.File {
		for prefix := range queue {
			if strings.HasPrefix(f.Name, prefix) {
				queue[prefix] = append(queue[prefix], f)
			}
		}
	}

	for _, prefix := range priority {
		for _, f := range queue[prefix] {
			fd, err := f.Open()
			if err != nil {
				return err
			}

			switch prefix {
			case `locations_`:
				err = loadLocations(fd)
			case `users_`:
				err = loadUsers(fd)
			case `visits_`:
				err = loadVisits(fd)
			}

			fd.Close()

			if err != nil {
				return err
			}
		}
	}

	return nil
}

func determineCurrentTime() {
	timeNow = time.Now()

	optionsTxt := path.Join(path.Dir(argv.zipPath), `options.txt`)
	if fd, err := os.Open(optionsTxt); err == nil {
		if err := loadOptions(fd); err != nil {
			log.Println(`Cannot read options.txt:`, err)
		}
		fd.Close()
	}

	timeNow = timeFloor(timeNow.In(time.UTC))
	log.Printf("NOW %d (%s)\n", timeNow.Unix(), timeNow.String())
}

func loadOptions(src io.Reader) (err error) {
	if line, err := bufio.NewReader(src).ReadString('\n'); err != nil {
		return err
	} else if ts, err := strconv.ParseUint(strings.TrimSpace(line), 10, 64); err != nil {
		return err
	} else {
		newTimeNow := time.Unix(int64(ts), 0)
		timeNow = newTimeNow
	}

	return nil
}

func loadUsers(src io.Reader) (err error) {
	buf.Reset()
	buf.ReadFrom(src)

	err = ParseData(buf.Bytes(), strUsers, func(item []byte) bool {
		var user User
		if !user.Parse(item) {
			return false
		} else if !indexUser.Add(&user) {
			return false
		}

		if dictStatistics.UserMaxId < user.Id {
			dictStatistics.UserMaxId = user.Id
		}
		dictStatistics.Users++

		return true
	})

	return
}

func loadLocations(src io.Reader) (err error) {
	buf.Reset()
	buf.ReadFrom(src)

	err = ParseData(buf.Bytes(), strLocations, func(item []byte) bool {
		var location Location
		if !location.Parse(item) {
			return false
		} else if !indexLocation.Add(&location) {
			return false
		}

		if dictStatistics.LocationMaxId < location.Id {
			dictStatistics.LocationMaxId = location.Id
		}
		dictStatistics.Locations++

		return true
	})

	return
}

func loadVisits(src io.Reader) (err error) {
	buf.Reset()
	buf.ReadFrom(src)

	err = ParseData(buf.Bytes(), strVisits, func(item []byte) bool {
		var visit Visit
		if !visit.Parse(item) {
			return false
		} else if !indexVisit.Add(&visit) {
			return false
		} else if user := indexUser.Get(visit.User); user == nil {
			// кривые даннные в исходной выборке?
			return false
		} else if location := indexLocation.Get(visit.Location); location == nil {
			// кривые даннные в исходной выборке?
			return false
		} else {
			user.cache.Add(location, &visit)
			location.cache.Add(location, &visit, user)
		}

		if dictStatistics.VisitMaxId < visit.Id {
			dictStatistics.VisitMaxId = visit.Id
		}
		dictStatistics.Visits++

		return true
	})

	return
}

func warming() {
	ageToTimestampWarming()
}
