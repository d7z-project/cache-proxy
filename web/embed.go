package web

import (
	"embed"
	"io/fs"
)

//go:embed dist/cache-proxy-web/browser/*
var assets embed.FS

func Assets() fs.FS {
	browser, err := fs.Sub(assets, "dist/cache-proxy-web/browser")
	if err != nil {
		panic(err)
	}
	return browser
}
