package socket

import (
	"net"
)

func HandleConn(conn net.Conn) {
	message := make(chan int)
	go HeartBeating(conn, message, 5)
	Handle(conn, message)
}

func HandleConnWithTimeout(conn net.Conn, timeout int) {
	message := make(chan int)
	go HeartBeating(conn, message, timeout)
	Handle(conn, message)
}

func Handle(conn net.Conn, message chan int) {
	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			return
		}

		Depack(buffer[:n])
		message <- 1
	}
}

