package app

import (
	"context"

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

var modePlanners = map[string]func(context.Context, *proxyruntime.InstancePlan) error{
	config.ModeAPK:     apk.Plan,
	config.ModeCargo:   cargo.Plan,
	config.ModeDEB:     deb.Plan,
	config.ModeFile:    file.Plan,
	config.ModeFlatpak: flatpak.Plan,
	config.ModeGit:     git.Plan,
	config.ModeGo:      gomod.Plan,
	config.ModeMaven:   maven.Plan,
	config.ModeNPM:     npm.Plan,
	config.ModeOCI:     oci.Plan,
	config.ModePacman:  pacman.Plan,
	config.ModePyPI:    pypi.Plan,
	config.ModeRPM:     rpm.Plan,
}
