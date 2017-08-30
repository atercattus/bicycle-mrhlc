package main

import (
	"net"
	"syscall"
)

const (
	EPOLLET        = 1 << 31 // в stdlib идет не того типа, как хотелось бы
	MaxEpollEvents = 64
	SO_REUSEPORT   = 15 // нет в stdlib
)

func socketCreateListener(port int) (serverFd int, err error) {
	addr := syscall.SockaddrInet4{Port: port}
	copy(addr.Addr[:], net.ParseIP(`0.0.0.0`).To4())

	serverFd, err = syscall.Socket(syscall.AF_INET, syscall.O_NONBLOCK|syscall.SOCK_STREAM, 0)
	if err != nil {
		return
	}

	if err = socketSetNonBlocking(serverFd); err != nil {
		syscall.Close(serverFd)
		return
	}

	if err = syscall.SetsockoptInt(serverFd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		syscall.Close(serverFd)
		return
	}

	if err = syscall.SetsockoptInt(serverFd, syscall.SOL_SOCKET, SO_REUSEPORT, 1); err != nil {
		syscall.Close(serverFd)
		return
	}

	if err = syscall.Bind(serverFd, &addr); err != nil {
		syscall.Close(serverFd)
		return
	} else if err = syscall.Listen(serverFd, syscall.SOMAXCONN); err != nil {
		syscall.Close(serverFd)
		return
	}

	return
}

func socketSetNonBlocking(fd int) error {
	return syscall.SetNonblock(fd, true)
}

func socketCreateListenerEpoll(serverFd int) (epollFd int, err error) {
	var event syscall.EpollEvent
	event.Events = syscall.EPOLLIN | EPOLLET
	event.Fd = int32(serverFd)

	epollFd, err = syscall.EpollCreate1(0)
	if err != nil {
		return 0, err
	} else if err = syscall.EpollCtl(epollFd, syscall.EPOLL_CTL_ADD, serverFd, &event); err != nil {
		syscall.Close(epollFd)
		return 0, err
	}

	return epollFd, nil
}
