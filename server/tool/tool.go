package tool

import (
	"io"
	"fmt"
	"strconv"
	"time"
	"crypto/md5"
)

func GenerateMD5(str string) string {
	w := md5.New()
	io.WriteString(w, str)
	md5Str := fmt.Sprintf("%x", w.Sum(nil))
	return md5Str
}

func GenerateUUID() string {
	return GenerateMD5(strconv.Itoa(int(time.Now().UnixNano())))
}