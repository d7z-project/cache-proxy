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

	"code.d7z.net/d7z-project/cache-proxy/pkg/utils"

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
	cfg := &models.Config{
		Gc: models.ConfigGc{
			Meta: 10 * time.Second,
			Blob: 24 * time.Hour,
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

	if cfg.ErrorHtml != "" {
		file, err := os.ReadFile(cfg.ErrorHtml)
		if err != nil {
			return err
		}
		parse, err := template.New(cfg.ErrorHtml).Parse(string(file))
		if err == nil {
			return err
		}
		worker.SetHtmlPage(parse)
	}
	if err != nil {
		return err
	}
	for name, cache := range cfg.Caches {
		log.Printf("添加反向代理路径 %s[%s]", name, strings.Join(cache.URLs, ","))
		target := services.NewTarget(name, cache.URLs...)
		for _, rule := range cache.Rules {
			if err = target.AddRule(rule.Regex, rule.Ttl, rule.Refresh); err != nil {
				return errors.Wrapf(err, "处理 %s 失败.", name)
			}
		}
		for _, replace := range cache.Replaces {
			if err = target.AddReplace(replace.Regex, replace.Old, replace.New); err != nil {
				return errors.Wrapf(err, "处理 %s 失败.", name)
			}
		}
		transport := cache.Transport
		if transport != nil {
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
			target.SetHttpClient(client)
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
	if cfg.PprofBind != "" {
		go func() {
			log.Printf("开启 pprof ，请访问 %s", cfg.PprofBind)
			log.Println(http.ListenAndServe(cfg.PprofBind, nil))
		}()
	}
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
