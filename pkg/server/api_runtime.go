package server

import (
	"net/http"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func (r *Runtime) publicInstancesAPI(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	r.mu.RLock()
	instances := config.CloneInstances(r.instances)
	r.mu.RUnlock()
	writeJSON(w, listInstanceSummaries(req, instances), nil)
}

func (r *Runtime) runtimeAPI(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	stats := r.stats.Snapshot()
	r.mu.RLock()
	response := runtimeResponse{
		Bind:          r.bind,
		Backend:       r.backend,
		AuthEnabled:   r.password != "",
		MetricsPath:   r.global.Metrics.Path,
		GCInterval:    r.global.Storage.GC.Blob.String(),
		ConfigVersion: r.global.Version,
		Generation:    r.generation,
		Instances:     len(r.instances),
		Handlers:      len(r.handlers),
		Requests:      stats.Total.Requests,
		Errors:        stats.Total.Errors,
		Upstreams:     stats.Total.UpstreamRequests,
	}
	r.mu.RUnlock()
	writeJSON(w, response, nil)
}

func (r *Runtime) metricsStatsAPI(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, r.stats.Snapshot(), nil)
}

func (r *Runtime) storageStatsAPI(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	stats, err := r.store.Stats(req.Context())
	writeJSON(w, stats, err)
}

func (r *Runtime) storageGCAPI(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	result, err := r.store.RunGC(req.Context(), blobfs.GCOptions{Compact: true})
	writeJSON(w, result, err)
}
