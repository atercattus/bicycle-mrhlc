package main

/*
#include <unistd.h>
*/
import "C"

import (
	"bytes"
	"fmt"
	"os"
	"runtime/debug"
	"time"
)

var (
	gcStatsBuf bytes.Buffer
	gcStats    debug.GCStats
)

func getGCStats() []byte {
	debug.ReadGCStats(&gcStats)

	gcStatsBuf.Reset()

	for _, p := range gcStats.Pause {
		fmt.Fprint(&gcStatsBuf, int64(p/time.Millisecond), ` `)
	}

	return gcStatsBuf.Bytes()
}

func getRSSMemory() (rss int64) {
	fd, err := os.Open(`/proc/self/statm`)
	if err != nil {
		return 0
	}
	defer fd.Close()

	var tmp int64

	if _, err := fmt.Fscanf(fd, `%d %d`, &tmp, &rss); err != nil {
		return 0
	}

	pagesize := int64(C.sysconf(C._SC_PAGESIZE))

	rss *= pagesize

	return
}
