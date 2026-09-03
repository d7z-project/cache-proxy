package main

import "net/http"

func (*fixtureServer) serveOCIBase(w http.ResponseWriter) {
	w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
	w.WriteHeader(http.StatusOK)
}
