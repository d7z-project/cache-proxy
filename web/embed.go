package web

import (
	"embed"
	"io/fs"
	"sync"
)

//go:embed dist/cache-proxy-web/browser/*
var assets embed.FS

var cachedFS fs.FS
var cachedFSOnce sync.Once

func Assets() fs.FS {
	cachedFSOnce.Do(func() {
		browser, err := fs.Sub(assets, "dist/cache-proxy-web/browser")
		if err != nil {
			panic(err)
		}
		cachedFS = browser
	})
	return cachedFS
}
