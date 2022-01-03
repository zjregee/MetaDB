package log

import (
	"log"
	"os"
)

var Logger *log.Logger

var logfile *os.File

func InitLog() {
	fileName := "server.log"
	var err error
	logfile, err = os.OpenFile(fileName, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalln("open file error!")
	}
	Logger = log.New(logfile, "", log.LstdFlags)
}

func CloseLog() {
	logfile.Close()
}