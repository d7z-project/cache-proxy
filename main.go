package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/template"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"gopkg.d7z.net/cache-proxy/pkg/utils"

	"gopkg.d7z.net/cache-proxy/pkg/models"
	"gopkg.d7z.net/cache-proxy/pkg/services"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

var conf = ""

func init() {
	flag.StringVar(&conf, "conf", "config.yaml", "config file")
	flag.Parse()
}

func mainExit() error {
	cfg := &models.Config{
		Gc: models.ConfigGc{
			Meta: 10 * time.Second,
			Blob: 24 * time.Hour,
		},
		Monitor: models.ConfigPrometheus{
			Bind: "127.0.0.1:8911",
			Path: "/metrics",
		},
	}
	if cfgB, err := os.ReadFile(conf); err != nil {
		return errors.Wrapf(err, "配置文件不存在 %s", conf)
	} else {
		if err := yaml.Unmarshal(cfgB, cfg); err != nil {
			return errors.Wrapf(err, "配置解析失败")
		}
	}
	worker, err := services.NewWorker(cfg.Backend, cfg.Gc.Meta, cfg.Gc.Blob)
	if err != nil {
		return err
	}
	if cfg.ErrorHtml != "" {
		file, err := os.ReadFile(cfg.ErrorHtml)
		if err != nil {
			return err
		}
		parse, err := template.New(cfg.ErrorHtml).Parse(string(file))
		if err != nil {
			return err
		}
		worker.SetHtmlPage(parse)
	}
	for name, cache := range cfg.Caches {
		log.Printf("添加反向代理路径 %s[%s]", name, strings.Join(cache.URLs, ","))
		target := services.NewTarget(name, cache.URLs...)
		for _, rule := range cache.Rules {
			if err = target.AddRule(rule.Regex, rule.Ttl, rule.Refresh); err != nil {
				return errors.Wrapf(err, "处理 %s 失败.", name)
			}
		}
		for _, include := range cache.RulesInclude {
			if ruleSet, ok := cfg.Rules[include]; ok {
				for _, rule := range ruleSet.Rules {
					if err = target.AddRule(rule.Regex, rule.Ttl, rule.Refresh); err != nil {
						return errors.Wrapf(err, "处理 %s (引用规则 %s) 失败.", name, include)
					}
				}
			} else {
				return errors.Errorf("引用规则 %s 未定义", include)
			}
		}
		for _, replace := range cache.Replaces {
			if err = target.AddReplace(replace.Regex, replace.Old, replace.New); err != nil {
				return errors.Wrapf(err, "处理 %s 失败.", name)
			}
		}
		if transport := cache.Transport; transport != nil {
			client := utils.DefaultHttpClientWrapper()
			if transport.Proxy != "" {
				proxyUrl, err := url.Parse(transport.Proxy)
				if err != nil {
					return errors.Wrapf(err, "处理 %s 失败.", name)
				}
				client.Transport = &http.Transport{
					Proxy: http.ProxyURL(proxyUrl),
				}
			}
			if transport.Timeout > 0 {
				client.Transport.(*http.Transport).DialContext = utils.DefaultDialContext(transport.Timeout)
			}
			if transport.UserAgent != "" {
				client.UserAgent = transport.UserAgent
			}
			if transport.Headers != nil {
				client.Headers = transport.Headers
			}
			target.SetHttpClient(client)
		}
		if err := worker.Bind(name, target); err != nil {
			return errors.Wrapf(err, "处理 %s 失败.", name)
		}
	}
	server := &http.Server{Addr: cfg.Bind, Handler: worker}
	monitorHandler := http.NewServeMux()
	monitorHandler.Handle(cfg.Monitor.Path, promhttp.Handler())
	monitorServer := &http.Server{Addr: cfg.Monitor.Bind, Handler: monitorHandler}
	log.Printf("服务器已启动，请访问 http://%s", cfg.Bind)
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		log.Printf("平台监控已启用.")
		err = monitorServer.ListenAndServe()
		if err != nil {
			log.Printf("monitor http server listen err: %v", err)
		}
	}()
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
		_ = server.Shutdown(context.Background())
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
