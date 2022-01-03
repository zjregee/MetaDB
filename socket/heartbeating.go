package socket

import (
	"net"
	"time"
)

func HeartBeating(conn net.Conn, channl chan int, timeout int) {
	for {
		select {
		case <-channl:
			conn.SetDeadline(time.Now().Add(time.Duration(timeout) * time.Second))
		case <-time.After(time.Second*5):
			conn.Close()
			return
		}
	}
}