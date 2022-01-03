package main
import (
	"MetaDB/kv/cmd"
	"MetaDB/kv"

	"flag"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/pelletier/go-toml"
)

// The param config means the config file path for rosedb.
// For the default config file, see config.toml.
var config = flag.String("config", "", "the config file for rosedb")

// The param dirPath means the persistent directory for db files and other config.
var dirPath = flag.String("dir_path", "", "the dir path for the database")

func main() {
	flag.Parse()

	// Set the config.
	var cfg kv.Config
	if *config == "" {
		log.Println("no config set, using the default config.")
		cfg = kv.DefaultConfig()
	} else {
		c, err := newConfigFromFile(*config)
		if err != nil {
			log.Printf("load config err : %+v\n", err)
			return
		}
		cfg = *c
	}

	if *dirPath == "" {
		log.Println("no dir path set, using the os tmp dir.")
	} else {
		cfg.DirPath = *dirPath
	}

	// Listen the server.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	server, err := cmd.NewServer(cfg)
	if err != nil {
		log.Printf("create rosedb server err: %+v\n", err)
		return
	}
	go server.Listen(cfg.Addr)

	<-sig
	server.Stop()
	log.Println("kvdb is ready to exit, bye...")
}

func newConfigFromFile(config string) (*kv.Config, error) {
	data, err := ioutil.ReadFile(config)
	if err != nil {
		return nil, err
	}

	var cfg = new(kv.Config)
	err = toml.Unmarshal(data, cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
