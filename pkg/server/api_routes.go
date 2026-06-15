package server

import (
	"net/http"
	"strings"
)

func (r *Runtime) serveAPI(w http.ResponseWriter, req *http.Request) {
	switch {
	case req.URL.Path == "/-/api/runtime":
		r.runtimeAPI(w, req)
	case req.URL.Path == "/-/api/instances":
		r.instancesCollectionAPI(w, req)
	case req.URL.Path == "/-/api/instances/export":
		r.instancesExportAPI(w, req)
	case req.URL.Path == "/-/api/instances/import":
		r.instancesImportAPI(w, req)
	case strings.HasPrefix(req.URL.Path, "/-/api/instances/"):
		r.instanceDocumentAPI(w, req, strings.TrimPrefix(req.URL.Path, "/-/api/instances/"))
	case req.URL.Path == "/-/api/metrics/stats":
		r.metricsStatsAPI(w, req)
	case req.URL.Path == "/-/api/storage/stats":
		r.storageStatsAPI(w, req)
	case req.URL.Path == "/-/api/storage/gc":
		r.storageGCAPI(w, req)
	case req.URL.Path == "/-/api/storage/cleanup":
		r.storageCleanupAPI(w, req)
	case req.URL.Path == "/-/api/cache/lookup":
		r.cacheLookupAPI(w, req)
	case req.URL.Path == "/-/api/system/reset":
		r.resetAPI(w, req)
	default:
		http.NotFound(w, req)
	}
}
