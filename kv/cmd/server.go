package cmd

import (
	"MetaDB/kv"

	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/tidwall/redcon"
)

type ExecCmdFunc func(*kv.KVDB, []string) (interface{}, error)

var ExecCmd = make(map[string]ExecCmdFunc)

func addExecCommand(cmd string, cmdFunc ExecCmdFunc) {
	ExecCmd[strings.ToLower(cmd)] = cmdFunc
}

type Server struct {
	server *redcon.Server
	db     *kv.KVDB
	closed bool
	mu     sync.Mutex
}

func NewServer(config kv.Config) (*Server, error) {
	db, err := kv.Open(config)
	if err != nil {
		return nil, err
	}
	return &Server{db: db}, nil
}

func (s *Server) Listen(addr string) {
	svr := redcon.NewServerNetwork("tcp", addr,
		func(conn redcon.Conn, cmd redcon.Command) {
			s.handleCmd(conn, cmd)
		},
		func(conn redcon.Conn) bool {
			return true
		},
		func(conn redcon.Conn, err error) {
		},
	)

	s.server = svr
	log.Println("rosedb is running, ready to accept connections.")
	if err := svr.ListenAndServe(); err != nil {
		log.Printf("listen and serve ocuurs error: %+v", err)
	}
}

func (s *Server) Stop() {
	if s.closed {
		return
	}
	s.mu.Lock()
	s.closed = true
	if err := s.server.Close(); err != nil {
		log.Printf("close redcon err: %+v\n", err)
	}
	if err := s.db.Close(); err != nil {
		log.Printf("close rosedb err: %+v\n", err)
	}
	s.mu.Unlock()
}

func (s *Server) handleCmd(conn redcon.Conn, cmd redcon.Command) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic when handle the cmd: %+v", r)
		}
	}()

	command := strings.ToLower(string(cmd.Args[0]))
	exec, exist := ExecCmd[command]
	if !exist {
		conn.WriteError(fmt.Sprintf("ERR unknown command '%s'", command))
		return
	}
	args := make([]string, 0, len(cmd.Args)-1)
	for i, bytes := range cmd.Args {
		if i == 0 {
			continue
		}
		args = append(args, string(bytes))
	}
	reply, err := exec(s.db, args)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteAny(reply)
}
