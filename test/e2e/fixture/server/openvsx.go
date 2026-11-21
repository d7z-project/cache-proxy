package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

func (s *fixtureServer) serveOpenVSX(w http.ResponseWriter, r *http.Request, revision fixtureRevision) bool {
	version := fmt.Sprintf("%d.0.0", revision.packageMajor)
	base := s.publicURL
	switch {
	case r.URL.Path == "/api/e2e/extension" || r.URL.Path == "/api/e2e/extension/alpine-arm64":
		target := "universal"
		metadataPath := "/api/e2e/extension/"
		if strings.HasSuffix(r.URL.Path, "/alpine-arm64") {
			target = "alpine-arm64"
			metadataPath += target + "/"
		}
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"namespace": "e2e", "name": "extension", "version": version, "targetPlatform": target,
			"versionAlias": []string{"latest"},
			"allVersions":  map[string]string{version: base + metadataPath + version},
			"files": map[string]string{
				"download": base + metadataPath + version + "/file/download",
				"manifest": base + metadataPath + version + "/file/manifest",
			},
			"homepage": "https://github.com/eclipse-openvsx/openvsx",
		})
		return true
	case strings.HasPrefix(r.URL.Path, "/api/e2e/extension/") && strings.HasSuffix(r.URL.Path, "/file/download"):
		target := ""
		if strings.Contains(r.URL.Path, "/alpine-arm64/") {
			target = "@alpine-arm64"
		}
		fixtureURL, _ := url.Parse(s.publicURL)
		w.Header().Set("Location", "http://localhost:"+fixtureURL.Port()+"/openvsx-cdn/e2e-extension-"+version+target+".vsix")
		w.WriteHeader(http.StatusFound)
		return true
	case strings.HasPrefix(r.URL.Path, "/api/e2e/extension/") && strings.HasSuffix(r.URL.Path, "/file/manifest"):
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"name":"extension","version":%q}`, version)
		return true
	case strings.HasPrefix(r.URL.Path, "/openvsx-cdn/"):
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("Content-Type", "application/octet-stream")
		switch {
		case strings.HasSuffix(r.URL.Path, "/unpkg-module.wasm"):
			_, _ = io.WriteString(w, "e2e-openvsx-unpkg")
		case strings.Contains(r.URL.Path, "@alpine-arm64"):
			_, _ = fmt.Fprintf(w, "e2e-openvsx-alpine-arm64-v%s", version)
		default:
			_, _ = fmt.Fprintf(w, "e2e-openvsx-v%s", version)
		}
		return true
	case r.URL.Path == "/vscode/unpkg/e2e/extension/"+version+"/extension":
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]string{base + "/vscode/unpkg/e2e/extension/" + version + "/extension/module.wasm"})
		return true
	case r.URL.Path == "/vscode/unpkg/e2e/extension/"+version+"/extension/module.wasm":
		fixtureURL, _ := url.Parse(s.publicURL)
		w.Header().Set("Location", "http://localhost:"+fixtureURL.Port()+"/openvsx-cdn/unpkg-module.wasm")
		w.WriteHeader(http.StatusFound)
		return true
	case r.URL.Path == "/vscode/gallery/extensionquery" && r.Method == http.MethodPost:
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"results":[{"extensions":[{"extensionId":"e2e.extension","versions":[{"version":%q,"assetUri":%q,"files":[{"assetType":"Microsoft.VisualStudio.Services.VSIXPackage","source":%q}]}]}]}]}`,
			version, base+"/vscode/asset/e2e/extension/"+version, base+"/api/e2e/extension/"+version+"/file/download")
		return true
	case r.URL.Path == "/vscode/item":
		w.Header().Set("Location", base+"/extension/e2e/extension")
		w.WriteHeader(http.StatusFound)
		return true
	case r.URL.Path == "/" || r.URL.Path == "/extension/e2e/extension":
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(w, `<!doctype html><html><head><script src="https://tracker.invalid/analytics.js"></script><script type="module" src="`+base+`/bundle-e2e.js"></script><link rel="stylesheet" href="/index-e2e.css"></head><body>Open VSX E2E</body></html>`)
		return true
	case r.URL.Path == "/bundle-e2e.js":
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("Content-Type", "application/javascript")
		_, _ = io.WriteString(w, `export const registry = "`+base+`/api";`)
		return true
	case r.URL.Path == "/index-e2e.css":
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("Content-Type", "text/css")
		_, _ = io.WriteString(w, `body { color: #222; }`)
		return true
	default:
		return false
	}
}
