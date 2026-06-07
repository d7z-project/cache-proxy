package server

import (
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"strings"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/web"
)

type configUpdateRequest struct {
	Generation uint64         `json:"generation"`
	Config     *config.Config `json:"config"`
}

type instancesExportResponse struct {
	Generation uint64                           `json:"generation"`
	Instances  map[string]config.InstanceConfig `json:"instances"`
}

type instancesImportRequest struct {
	Generation uint64                           `json:"generation"`
	Replace    bool                             `json:"replace"`
	Name       string                           `json:"name,omitempty"`
	Instance   *config.InstanceConfig           `json:"instance,omitempty"`
	Instances  map[string]config.InstanceConfig `json:"instances,omitempty"`
}

func (r *Runtime) configAPI(resp http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		snapshot, err := r.Snapshot(req.Context())
		writeJSON(resp, snapshot, err)
	case http.MethodPut:
		var input configUpdateRequest
		if err := json.NewDecoder(http.MaxBytesReader(resp, req.Body, 1<<20)).Decode(&input); err != nil {
			writeError(resp, http.StatusBadRequest, err)
			return
		}
		snapshot, err := r.UpdateConfig(req.Context(), input.Generation, input.Config)
		if err != nil {
			var conflict conflictError
			if errors.As(err, &conflict) {
				writeError(resp, http.StatusConflict, err)
				return
			}
			writeError(resp, http.StatusBadRequest, err)
			return
		}
		writeJSON(resp, snapshot, nil)
	default:
		resp.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (r *Runtime) validateAPI(resp http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var cfg config.Config
	if err := json.NewDecoder(http.MaxBytesReader(resp, req.Body, 1<<20)).Decode(&cfg); err != nil {
		writeError(resp, http.StatusBadRequest, err)
		return
	}
	r.preserveStartupOnlyConfig(&cfg)
	if err := r.validateConfig(&cfg); err != nil {
		writeError(resp, http.StatusBadRequest, err)
		return
	}
	writeJSON(resp, map[string]bool{"valid": true}, nil)
}

func (r *Runtime) resetAPI(resp http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	r.mu.RLock()
	generation := r.generation
	r.mu.RUnlock()
	snapshot, err := r.UpdateConfig(req.Context(), generation, DefaultConfig())
	writeJSON(resp, snapshot, err)
}

func (r *Runtime) instancesAPI(resp http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	type item struct {
		Name string `json:"name"`
		Mode string `json:"mode"`
		Path string `json:"path,omitempty"`
		Bind string `json:"bind,omitempty"`
	}
	r.mu.RLock()
	instances := r.config.Instances
	items := []item{}
	for _, name := range sortedInstanceNames(instances) {
		cfg := instances[name]
		items = append(items, item{Name: name, Mode: cfg.Mode, Path: cfg.Listen.Path, Bind: cfg.Listen.Bind})
	}
	r.mu.RUnlock()
	writeJSON(resp, items, nil)
}

func (r *Runtime) instancesExportAPI(resp http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	name := strings.TrimSpace(req.URL.Query().Get("name"))
	r.mu.RLock()
	generation := r.generation
	result := map[string]config.InstanceConfig{}
	if name != "" {
		instance, ok := r.config.Instances[name]
		if !ok {
			r.mu.RUnlock()
			writeError(resp, http.StatusNotFound, errors.New("instance not found"))
			return
		}
		result[name] = instance
	} else {
		for _, instanceName := range sortedInstanceNames(r.config.Instances) {
			result[instanceName] = r.config.Instances[instanceName]
		}
	}
	r.mu.RUnlock()
	writeJSON(resp, instancesExportResponse{Generation: generation, Instances: result}, nil)
}

func (r *Runtime) instancesImportAPI(resp http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var input instancesImportRequest
	if err := json.NewDecoder(http.MaxBytesReader(resp, req.Body, 1<<20)).Decode(&input); err != nil {
		writeError(resp, http.StatusBadRequest, err)
		return
	}
	instances := input.Instances
	if instances == nil && input.Name != "" && input.Instance != nil {
		instances = map[string]config.InstanceConfig{input.Name: *input.Instance}
	}
	if len(instances) == 0 {
		writeError(resp, http.StatusBadRequest, errors.New("no instances to import"))
		return
	}

	r.mu.RLock()
	currentGeneration := r.generation
	next := cloneConfig(r.config)
	r.mu.RUnlock()
	if input.Generation != currentGeneration {
		writeError(resp, http.StatusConflict, conflictError("config generation changed"))
		return
	}
	for name, instance := range instances {
		if _, exists := next.Instances[name]; exists && !input.Replace {
			writeError(resp, http.StatusConflict, errors.New("instance already exists: "+name))
			return
		}
		next.Instances[name] = instance
	}
	snapshot, err := r.UpdateConfig(req.Context(), input.Generation, next)
	if err != nil {
		var conflict conflictError
		if errors.As(err, &conflict) {
			writeError(resp, http.StatusConflict, err)
			return
		}
		writeError(resp, http.StatusBadRequest, err)
		return
	}
	writeJSON(resp, snapshot, nil)
}

func (r *Runtime) runtimeAPI(resp http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	stats := r.stats.Snapshot()
	r.mu.RLock()
	generation := r.generation
	instanceCount := len(r.config.Instances)
	serverCount := len(r.servers)
	r.mu.RUnlock()
	writeJSON(resp, map[string]any{
		"adminBind":   r.adminBind,
		"proxyBind":   r.proxyBind,
		"backend":     r.backend,
		"metricsBind": r.metricsBind,
		"metricsPath": r.metricsPath,
		"gcInterval":  r.gcInterval.String(),
		"generation":  generation,
		"instances":   instanceCount,
		"servers":     serverCount,
		"requests":    stats.Total.Requests,
		"errors":      stats.Total.Errors,
		"upstreams":   stats.Total.UpstreamRequests,
	}, nil)
}

func (r *Runtime) metricsStatsAPI(resp http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSON(resp, r.stats.Snapshot(), nil)
}

func (r *Runtime) storageStatsAPI(resp http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	stats, err := r.store.Stats(req.Context())
	writeJSON(resp, stats, err)
}

func (r *Runtime) storageGCAPI(resp http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	result, err := r.store.RunGC(req.Context(), blobfs.GCOptions{Compact: true})
	writeJSON(resp, result, err)
}

func (r *Runtime) webUI(resp http.ResponseWriter, req *http.Request) {
	assets := web.Assets()
	filePath := strings.TrimPrefix(req.URL.Path, "/")
	if filePath == "" {
		filePath = "index.html"
	}
	if stat, err := fs.Stat(assets, filePath); err == nil && !stat.IsDir() {
		http.FileServerFS(assets).ServeHTTP(resp, req)
		return
	}
	req.URL.Path = "/index.html"
	http.FileServerFS(assets).ServeHTTP(resp, req)
}

func writeJSON(resp http.ResponseWriter, value any, err error) {
	if err != nil {
		writeError(resp, http.StatusInternalServerError, err)
		return
	}
	resp.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(resp).Encode(value)
}

func writeError(resp http.ResponseWriter, status int, err error) {
	resp.Header().Set("Content-Type", "application/json")
	resp.WriteHeader(status)
	_ = json.NewEncoder(resp).Encode(map[string]string{"error": err.Error()})
}
