package main

import (
	"bytes"
	"log"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
)

type (
	Method             int
	parseRequestStatus int
	parseRequestState  int
)

const (
	MethodGET = Method(iota)
	MethodPOST
)

const (
	parseRequestStatusOk = parseRequestStatus(iota)
	parseRequestStatusBadRequest
	parseRequestStatusNeedMore
)

const (
	parseRequestStateBegin = parseRequestState(iota)
	parseRequestStateHeaders
	parseRequestStateBody
)

type (
	RequestCtx struct {
		state         parseRequestState
		contentLength int
		keepAlive     bool

		Method Method
		Path   []byte
		Body   []byte

		ResponseStatus int
		ResponseBody   []byte

		inputBuf     []byte
		inputBufOffs int
		outputBuf    []byte

		UserBuf []byte // может использоваться внутри RequestHandler как угодно, сервер его не трогает
	}

	RequestHandler func(ctx *RequestCtx)

	HTTPServer struct {
		Handler                RequestHandler
		httpCurrentConnections int32
	}
)

func (c *RequestCtx) Reset() {
	c.state = parseRequestStateBegin
	c.contentLength = 0
	c.keepAlive = false

	c.Method = MethodGET
	c.Path = nil
	c.Body = nil

	c.ResponseStatus = 200
	c.ResponseBody = nil

	c.inputBufOffs = 0
	if cap(c.inputBuf) == 0 {
		c.inputBuf = make([]byte, 16*1024, 16*1024)
	} else {
		c.inputBuf = c.inputBuf[:]
	}

	if cap(c.outputBuf) == 0 {
		c.outputBuf = make([]byte, 0, 16*1024)
	} else {
		c.outputBuf = c.outputBuf[:0]
	}

	if cap(c.UserBuf) == 0 {
		c.UserBuf = make([]byte, 0, 16*1024)
	} else {
		c.UserBuf = c.UserBuf[:0]
	}
}

func (s *HTTPServer) GetCurrentConnections() int32 {
	return atomic.LoadInt32(&s.httpCurrentConnections)
}

func (s *HTTPServer) ListenAndServe(port int) error {
	cpu := runtime.NumCPU()
	if cpu > 4 {
		cpu = 4
	}
	log.Println(`Use`, cpu, `listeners`)

	serverFds := make([]int, 0, cpu)
	epollFds := make([]int, 0, cpu)

	defer func() {
		for i := range serverFds {
			serverFd := serverFds[i]
			epollFd := epollFds[i]

			syscall.Close(epollFd)
			syscall.Close(serverFd)
		}
	}()

	for i := 1; i <= cpu; i++ {
		if serverFd, err := socketCreateListener(port); err != nil {
			return err
		} else if epollFd, err := socketCreateListenerEpoll(serverFd); err != nil {
			syscall.Close(serverFd)
			return err
		} else {
			serverFds = append(serverFds, serverFd)
			epollFds = append(epollFds, epollFd)
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(serverFds))
	for i := range serverFds {
		serverFd := serverFds[i]
		epollFd := epollFds[i]
		go s.epollLoop(epollFd, serverFd)
	}
	wg.Wait()

	return nil
}

func (s *HTTPServer) epollLoop(epollFd, serverFd int) {
	var (
		epollEvent  syscall.EpollEvent
		epollEvents [MaxEpollEvents]syscall.EpollEvent

		activeCtx = make(map[int]*RequestCtx, 10) // 2000 ?
		usedCtx   []*RequestCtx

		ctx *RequestCtx
	)

	closeClientFd := func(fd int) {
		if ctx, ok := activeCtx[fd]; !ok {
			return
		} else {
			syscall.Close(fd)
			delete(activeCtx, fd)
			usedCtx = append(usedCtx, ctx)
		}
	}

	for {
		nEvents, err := syscall.EpollWait(epollFd, epollEvents[:], -1)
		if err != nil {
			if errno, ok := err.(syscall.Errno); ok && (errno == syscall.EINTR) {
				continue
			}

			log.Println(`EpollWait: `, err)
			break
		}

		for ev := 0; ev < nEvents; ev++ {
			fd := int(epollEvents[ev].Fd)
			events := epollEvents[ev].Events

			if (events&syscall.EPOLLERR != 0) || (events&syscall.EPOLLHUP != 0) || (events&syscall.EPOLLIN == 0) {
				closeClientFd(fd)
				continue
			} else if fd == serverFd {
				for {
					connFd, _, err := syscall.Accept(serverFd)

					if err != nil {
						if errno, ok := err.(syscall.Errno); ok {
							if errno == syscall.EAGAIN {
								// обработаны все новые коннекты
							} else {
								log.Printf("Accept: errno: %v\n", errno)
							}
						} else {
							log.Printf("Accept: %T %s\n", err, err)
						}
						break
					} else if err := socketSetNonBlocking(connFd); err != nil {
						log.Println("setSocketNonBlocking: ", err)
						break
					}

					epollEvent.Events = syscall.EPOLLIN | EPOLLET // | syscall.EPOLLOUT
					epollEvent.Fd = int32(connFd)
					if err := syscall.EpollCtl(epollFd, syscall.EPOLL_CTL_ADD, connFd, &epollEvent); err != nil {
						log.Println("EpollCtl: ", err)
						break
					}
				}

				continue
			}

			// обработка основых соединений

			var ok bool
			if ctx, ok = activeCtx[fd]; !ok {
				if l := len(usedCtx); l > 0 {
					ctx = usedCtx[l-1]
					usedCtx = usedCtx[:l-1]
				} else {
					ctx = &RequestCtx{}
				}
				ctx.Reset()
				activeCtx[fd] = ctx
			}

			for {
				nbytes, err := syscall.Read(fd, ctx.inputBuf[ctx.inputBufOffs:])

				if err != nil {
					allOk := false
					if errno, ok := err.(syscall.Errno); ok {
						if errno == syscall.EAGAIN {
							// обработаны все новые данные
							allOk = true
						} else if errno == syscall.EBADF {
							// видимо, соединение уже закрылось и так чуть раньше по другому условию
						} else {
							log.Printf("Read: unknown errno: %v\n", errno)
						}
					} else {
						log.Printf("Read: unknown error type %T: %s\n", err, err)
					}

					if !allOk {
						closeClientFd(fd)
					}

					break
				}

				if nbytes > 0 {
					status, bufNew := s.parseRequest(ctx.inputBuf[ctx.inputBufOffs:ctx.inputBufOffs+nbytes], ctx)

					switch status {
					case parseRequestStatusOk:
						if s.Handler != nil {
							s.Handler(ctx)
						}
					case parseRequestStatusNeedMore:
						ctx.inputBufOffs += nbytes
						continue
					case parseRequestStatusBadRequest:
						ctx.ResponseStatus = 400
					}

					buf := s.buildResponse(ctx)
					if n, err := syscall.Write(fd, buf); err != nil {
						log.Println(`Write`, err)
					} else if n != len(buf) {
						log.Println(`Write`, n, `!=`, len(buf))
					}

					if ctx.Method == MethodPOST {
						// яндекс.Танк не умеет в нормальные POST запросы, присылая два лишних байта "\r\n"
						closeClientFd(fd)
						// ToDo: вычитывать их и работать дальше? ;)
						break
					} else {
						ctx.Reset()

						if len(bufNew) > 0 {
							log.Println(`buffer tail`, len(bufNew), string(bufNew))
							copy(ctx.inputBuf, bufNew)
							ctx.inputBufOffs = len(bufNew)
						} else {
							ctx.inputBufOffs = 0
						}
					}

				} else {
					// соединение закрылось
					closeClientFd(fd)
				}
			}
		}
	}
}

func (s *HTTPServer) buildResponse(ctx *RequestCtx) []byte {
	var line []byte
	switch ctx.ResponseStatus {
	case 200:
		line = line200
	case 400:
		line = line400
	case 404:
		line = line404
	default:
		line = line500
	}

	tmpBuf := ctx.outputBuf

	// формирование ответа
	tmpBuf = append(tmpBuf[:0], line...)

	tmpBuf = append(tmpBuf, "Content-Type: application/json\r\nServer: yocto_http\r\n"...)

	tmpBuf = append(tmpBuf, "Content-Length: "...)
	tmpBuf = strconv.AppendUint(tmpBuf, uint64(len(ctx.ResponseBody)), 10)
	tmpBuf = append(tmpBuf, "\r\n"...)

	if ctx.keepAlive {
		tmpBuf = append(tmpBuf, "Connection: keep-alive\r\n"...)
	}

	tmpBuf = append(tmpBuf, "\r\n"...)

	if ctx.ResponseBody != nil {
		tmpBuf = append(tmpBuf, ctx.ResponseBody...)
	}

	ctx.outputBuf = tmpBuf // на случай расширения буфера

	return tmpBuf
}

func (s *HTTPServer) parseRequest(buf []byte, ctx *RequestCtx) (parseRequestStatus, []byte) {
	var idx int

	for {
		switch ctx.state {
		case parseRequestStateBegin:
			if idx = bytes.IndexByte(buf, '\n'); idx == -1 {
				return parseRequestStatusNeedMore, buf
			}

			ctx.state = parseRequestStateHeaders

			// GET /path/to/file HTTP/1.1
			line := buf[:idx-1] // \r\n
			buf = buf[idx+1:]
			if idx = bytes.IndexByte(line, ' '); idx == -1 {
				return parseRequestStatusBadRequest, buf
			}

			// GET
			if method := line[0:idx]; bytes.Equal(method, strGET) {
				ctx.Method = MethodGET
			} else if bytes.Equal(method, strPOST) {
				ctx.Method = MethodPOST
			} else {
				return parseRequestStatusBadRequest, buf
			}
			line = line[idx+1:]

			// /path/to/file
			if idx = bytes.IndexByte(line, ' '); idx == -1 {
				return parseRequestStatusBadRequest, buf
			}
			ctx.Path = line[:idx]

			// HTTP/1.1 поддерживает keep-alive по умолчанию
			if bytes.HasSuffix(line, str11) {
				ctx.keepAlive = true
			}

		case parseRequestStateHeaders:
			if idx = bytes.IndexByte(buf, '\n'); idx == -1 {
				return parseRequestStatusNeedMore, buf
			}

			line := buf[:idx-1] // \r\n
			buf = buf[idx+1:]
			if len(line) == 0 {
				if ctx.contentLength == 0 {
					// все, распарсили запрос
					return parseRequestStatusOk, buf
				} else {
					ctx.state = parseRequestStateBody
				}
			} else if idx = bytes.IndexByte(line, ':'); idx == -1 {
				return parseRequestStatusBadRequest, buf
			} else {
				key, value := line[:idx], bytesTrimLeftInplace(line[idx+1:])
				bytesToLowerInplace(key)

				if bytes.Equal(key, strContentLength) {
					if i64, ok := byteSliceToInt64(value); !ok {
						return parseRequestStatusBadRequest, buf
					} else {
						ctx.contentLength = int(i64)
					}
				} else if bytes.Equal(key, strConnection) {
					bytesToLowerInplace(value)
					if bytes.Equal(value, strKeepAlive) {
						ctx.keepAlive = true
					} else if bytes.Equal(value, strClose) {
						ctx.keepAlive = false
					}
				}
			}

		case parseRequestStateBody:
			l := ctx.contentLength
			if len(buf) < l {
				return parseRequestStatusNeedMore, buf
			}

			ctx.Body = buf[:l]
			buf = buf[l:]

			return parseRequestStatusOk, buf

		default:
			log.Println(`Bug in code: unexpected parse state`)
			return parseRequestStatusBadRequest, buf
		}
	}

	log.Println(`Bug in code: after of HTTPServer.parseRequest loop`)
	return parseRequestStatusBadRequest, buf
}
