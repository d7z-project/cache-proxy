package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

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
	worker, err := services.NewWorker(cfg.Backend, cfg.Gc)
	if err != nil {
		return err
	}
	for name, cache := range cfg.Caches {
		log.Printf("添加反向代理路径 %s[%s]", name, strings.Join(cache.URLs, ","))
		target := services.NewTarget(cache.URLs...)
		for _, rule := range cache.Rules {
			err := target.AddRule(rule.Regex, rule.Ttl, rule.Refresh)
			if err != nil {
				return errors.Wrapf(err, "处理 %s 失败.", name)
			}
		}
		if err := worker.Bind(name, target); err != nil {
			return errors.Wrapf(err, "处理 %s 失败.", name)
		}
	}
	server := &http.Server{Addr: cfg.Bind, Handler: worker}
	log.Printf("服务器已启动，请访问 http://%s", cfg.Bind)
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		var err error
		sig := <-sigs
		log.Printf("停止反向代理工作区 [sig:%d]", sig)
		if err = worker.Close(); err != nil {
			log.Println(err)
		}
		log.Printf("停止反向代理服务")
		if err = server.Shutdown(context.Background()); err != nil {
			log.Println(err)
		}
		done <- true
	}()
	err = server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		sigs <- syscall.SIGTERM
	}
	<-done
	if !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func main() {
	if err := mainExit(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
