package web

import (
	"embed"
	"io/fs"
)

//go:embed dist/cache-proxy-web/browser/*
var assets embed.FS

var cachedFS fs.FS

func Assets() fs.FS {
	if cachedFS != nil {
		return cachedFS
	}
	browser, err := fs.Sub(assets, "dist/cache-proxy-web/browser")
	if err != nil {
		panic(err)
	}
	cachedFS = browser
	return cachedFS
}
