package app

import (
	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/apk"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/cargo"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/deb"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/file"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/flatpak"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/git"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/gomod"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/maven"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/npm"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/oci"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/pacman"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/pypi"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/rpm"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

func builtinDrivers() map[string]proxyruntime.ModeDriver {
	return map[string]proxyruntime.ModeDriver{
		config.ModeAPK:     apk.NewDriver(),
		config.ModeCargo:   cargo.NewDriver(),
		config.ModeDEB:     deb.NewDriver(),
		config.ModeFile:    file.NewDriver(),
		config.ModeFlatpak: flatpak.NewDriver(),
		config.ModeGit:     git.NewDriver(),
		config.ModeGo:      gomod.NewDriver(),
		config.ModeMaven:   maven.NewDriver(),
		config.ModeNPM:     npm.NewDriver(),
		config.ModeOCI:     oci.NewDriver(),
		config.ModePacman:  pacman.NewDriver(),
		config.ModePyPI:    pypi.NewDriver(),
		config.ModeRPM:     rpm.NewDriver(),
	}
}
