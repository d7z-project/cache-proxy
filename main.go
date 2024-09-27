package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"code.d7z.net/d7z-project/cache-proxy/pkg/models"
	"code.d7z.net/d7z-project/cache-proxy/pkg/services"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

var conf = ""

func init() {
	flag.StringVar(&conf, "conf", "config.yaml", "config file")
}

func mainExit() error {
	flag.Parse()
	cfg := &models.Config{}
	if cfgB, err := os.ReadFile(conf); err != nil {
		return errors.Wrapf(err, "配置文件不存在 %s", conf)
	} else {
		if err := yaml.Unmarshal(cfgB, cfg); err != nil {
			return errors.Wrapf(err, "配置解析失败")
		}
	}
	worker, err := services.NewWorker(cfg.Backend)
	if err != nil {
		return err
	}
	for s, cache := range cfg.Caches {
		//cache.
	}
	http.Handle("/", worker)
	log.Printf("服务器已启动，请访问 http://%s", cfg.Bind)
	return http.ListenAndServe(cfg.Bind, nil)
}

func main() {
	if err := mainExit(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
