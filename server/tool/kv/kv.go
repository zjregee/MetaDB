package kv

import (
	"MetaDB/server/tool/log"

	"fmt"
	
	"github.com/gomodule/redigo/redis"
)

const (
	host = "127.0.0.1"
	port = 5200
)

var conn redis.Conn

func InitKV() error {
	var err error
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err = redis.Dial("tcp", addr)
	if err != nil {
		log.Logger.Println("tcp dial err: ", err)
		return err
	}
	return nil
}

func HGET(key, field string) (string, error) {
	return "", nil
}

func HGETALL(key string) ([]string, error) {
	return []string{}, nil
}

func HSET(key, field string, value string) error {
	return nil
}

func HSETNX(key, field string, value string) error {
	return nil
}

func HDEL(key, field string) error {
	return nil
}

func HLEN(key string) int {
	return 0
}

func HEXIST(key, value string) (bool, error) {	
	return false, nil
}

func HKEYS(key string) ([]string, error) {
	return []string{}, nil
}

func HVALS(key string) ([]string, error) {
	return []string{}, nil
}

func DoCommand(command string, args []interface{}) (interface{}, error) {
	return conn.Do(command, args...)
}

func ParseReply(rawResp interface{}) string {
	switch reply := rawResp.(type) {
	case []byte:
		return string(reply)
	case string:
		return reply
	case nil:
		return "(nil)"
	}
	return ""
}