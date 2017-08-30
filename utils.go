package main

import (
	"bytes"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"
)

var (
	bytesPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 4094)
		},
	}
)

func byteSliceToInt64(s []byte) (res int64, ok bool) {
	sign := len(s) > 0 && s[0] == '-'
	if sign {
		s = s[1:]
	}

	ok = true

	res = 0
	for _, c := range s {
		if v := int64(c - '0'); v < 0 || v > 9 {
			ok = false
			break
		} else {
			res = res*10 + v
		}
	}

	if sign {
		res = -res
	}

	return
}

func utf8UnescapedLen(b []byte) int {
	if len(b) == 0 {
		return 0
	}
	return utf8.RuneCount(utf8Unescaped(b))
}

// хак для перевода экранированных строк вида "\u1234\u5678" в нормальный юникод
// выделяет память под ответ
func utf8Unescaped(b []byte) []byte {
	buf := bytesPool.Get().([]byte)[:0]

	var tmp [4]byte

	i, l := 0, len(b)
	for i < l {
		ch := b[i]

		// \u1234

		// в случае любых ошибок просто пропускаем один байт
		if ch != '\\' {
		} else if (i >= l-4) || b[i+1] != 'u' {
		} else if r, err := strconv.ParseUint(string(b[i+2:i+6]), 16, 64); err != nil {
		} else {
			n := utf8.EncodeRune(tmp[:], rune(r))
			buf = append(buf, tmp[:n]...)
			i += 6
			continue
		}

		buf = append(buf, ch)
		i++
	}

	res := append([]byte{}, buf...)

	bytesPool.Put(buf)

	return res
}

func timeFloor(t time.Time) time.Time {
	year, month, day := t.Date()
	t = time.Date(year, month, day, 0, 0, 0, 0, t.Location())
	return t
}

var agesTimestamps []int64

func ageToTimestampWarming() {
	for age := 0; age <= 200; age++ {
		agesTimestamps = append(agesTimestamps, ageToTimestamp(int32(age)))
	}
}

func ageToTimestamp(age int32) int64 {
	if (age >= 0) && int(age) < len(agesTimestamps) {
		return agesTimestamps[age]
	}

	year, month, day := timeNow.Date()
	ts := time.Date(year-int(age), month, day, 0, 0, 0, 0, timeNow.Location())
	return ts.Unix()
}

func bytesToLowerInplace(buf []byte) {
	for i, ch := range buf {
		if ch >= 'A' && ch <= 'Z' {
			buf[i] += 'a' - 'A'
		}
	}
}

func bytesTrimLeftInplace(buf []byte) []byte {
	i, l := 0, len(buf)
	for ; i < l && buf[i] == ' '; i++ {
	}
	return buf[i:]
}

// скопировано из github.com/valyala/fasthttp
var hex2intTable = func() []byte {
	b := make([]byte, 255)
	for i := byte(0); i < 255; i++ {
		c := byte(16)
		if i >= '0' && i <= '9' {
			c = i - '0'
		} else if i >= 'a' && i <= 'f' {
			c = i - 'a' + 10
		} else if i >= 'A' && i <= 'F' {
			c = i - 'A' + 10
		}
		b[i] = c
	}
	return b
}()

// скопировано из github.com/valyala/fasthttp
func urlDecode(dst, src []byte) []byte {
	if bytes.IndexByte(src, '%') < 0 && bytes.IndexByte(src, '+') < 0 {
		// fast path: src doesn't contain encoded chars
		return append(dst, src...)
	}

	// slow path
	for i := 0; i < len(src); i++ {
		c := src[i]
		if c == '%' {
			if i+2 >= len(src) {
				return append(dst, src[i:]...)
			}
			x2 := hex2intTable[src[i+2]]
			x1 := hex2intTable[src[i+1]]
			if x1 == 16 || x2 == 16 {
				dst = append(dst, '%')
			} else {
				dst = append(dst, x1<<4|x2)
				i += 2
			}
		} else if c == '+' {
			dst = append(dst, ' ')
		} else {
			dst = append(dst, c)
		}
	}
	return dst
}
