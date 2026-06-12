package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
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
		if snapshot != nil {
			snapshot.Config = snapshot.Config.Redacted()
		}
		writeJSON(resp, snapshot, err)
	case http.MethodPut:
		var input configUpdateRequest
		if err := json.NewDecoder(http.MaxBytesReader(resp, req.Body, int64(defaultConfigMaxBytes()))).Decode(&input); err != nil {
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
		if snapshot != nil {
			snapshot.Config = snapshot.Config.Redacted()
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
	if err := json.NewDecoder(http.MaxBytesReader(resp, req.Body, int64(defaultConfigMaxBytes()))).Decode(&cfg); err != nil {
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

type instanceSummary struct {
	Name string `json:"name"`
	Mode string `json:"mode"`
	Path string `json:"path,omitempty"`
	Bind string `json:"bind,omitempty"`
}

func (r *Runtime) listInstanceSummaries() []instanceSummary {
	r.mu.RLock()
	instances := r.config.Instances
	r.mu.RUnlock()
	result := make([]instanceSummary, 0, len(instances))
	for _, name := range sortedInstanceNames(instances) {
		cfg := instances[name]
		result = append(result, instanceSummary{Name: name, Mode: cfg.Mode, Path: cfg.Listen.Path, Bind: cfg.Listen.Bind})
	}
	return result
}

func (r *Runtime) publicInstancesAPI(resp http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSON(resp, r.listInstanceSummaries(), nil)
}

func (r *Runtime) instancesAPI(resp http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSON(resp, r.listInstanceSummaries(), nil)
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
		result[name] = instance.Redacted()
	} else {
		for _, instanceName := range sortedInstanceNames(r.config.Instances) {
			result[instanceName] = r.config.Instances[instanceName].Redacted()
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
	if err := json.NewDecoder(http.MaxBytesReader(resp, req.Body, int64(defaultConfigMaxBytes()))).Decode(&input); err != nil {
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
		if _, exists := next.Instances[name]; exists {
			if !input.Replace {
				writeError(resp, http.StatusConflict, errors.New("instance already exists: "+name))
				return
			}
		} else if instance.OCI != nil && instance.OCI.Auth != nil &&
			(instance.OCI.Auth.Password == "***" || instance.OCI.Auth.Token == "***") {
			writeError(resp, http.StatusBadRequest, errors.New("new instance "+name+" contains masked credentials"))
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
	handlerCount := len(r.handlers)
	r.mu.RUnlock()
	writeJSON(resp, map[string]any{
		"bind":        r.bind,
		"backend":     r.backend,
		"authEnabled": r.password != "",
		"metricsPath": r.metricsPath,
		"gcInterval":  r.gcInterval.String(),
		"generation":  generation,
		"instances":   instanceCount,
		"handlers":    handlerCount,
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

type cacheLookupResult struct {
	Instance    string `json:"instance"`
	Mode        string `json:"mode"`
	Path        string `json:"path"`
	ObjectPath  string `json:"objectPath"`
	Policy      string `json:"policy"`
	FreshFor    string `json:"freshFor"`
	ExpireAfter string `json:"expireAfter"`
	Generation  uint64 `json:"generation"`
	Cached      bool   `json:"cached"`
	CachedAt    string `json:"cachedAt,omitempty"`
	ExpiresAt   string `json:"expiresAt,omitempty"`
	Fresh       bool   `json:"fresh"`
}

func (r *Runtime) cacheLookupAPI(resp http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		resp.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	instanceName := strings.TrimSpace(req.URL.Query().Get("instance"))
	lookupPath := strings.TrimSpace(req.URL.Query().Get("path"))
	if instanceName == "" || lookupPath == "" {
		writeError(resp, http.StatusBadRequest, errors.New("instance and path are required"))
		return
	}

	r.mu.RLock()
	inst, ok := r.config.Instances[instanceName]
	generation := r.generation
	r.mu.RUnlock()
	if !ok {
		writeError(resp, http.StatusNotFound, errors.New("instance not found"))
		return
	}

	resolver, err := newResolver(inst)
	if err != nil {
		writeError(resp, http.StatusBadRequest, err)
		return
	}

	var route proxy.Route
	if lr, ok := resolver.(proxy.LookupResolver); ok {
		route, err = lr.ResolveLookup(lookupPath)
	} else {
		fakeReq, _ := http.NewRequest("GET", lookupPath, nil)
		fakeReq.URL.Path = "/" + lookupPath
		route, err = resolver.Resolve(fakeReq)
	}
	if err != nil {
		writeError(resp, http.StatusBadRequest, err)
		return
	}

	result := cacheLookupResult{
		Instance:    instanceName,
		Mode:        inst.Mode,
		Path:        lookupPath,
		ObjectPath:  route.ObjectPath,
		Policy:      route.Policy,
		Generation:  generation,
	}
	freshFor := route.FreshFor
	if freshFor <= 0 {
		freshFor = inst.Cache.FreshFor
	}
	if freshFor > 0 {
		result.FreshFor = freshFor.String()
	}
	expireAfter := route.ExpireAfter
	if expireAfter <= 0 {
		expireAfter = inst.ExpireAfter
	}
	if expireAfter > 0 {
		result.ExpireAfter = expireAfter.String()
	}

	cached, cachedAt, expiresAt, fresh := r.checkCacheStatus(req.Context(), instanceName, route, inst)
	result.Cached = cached
	result.Fresh = fresh
	if !cachedAt.IsZero() {
		result.CachedAt = cachedAt.UTC().Format(time.RFC3339)
	}
	if !expiresAt.IsZero() {
		result.ExpiresAt = expiresAt.UTC().Format(time.RFC3339)
	}

	writeJSON(resp, result, nil)
}

func (r *Runtime) checkCacheStatus(ctx context.Context, instanceName string, route proxy.Route, inst config.InstanceConfig) (bool, time.Time, time.Time, bool) {
	reader, err := r.store.OpenObject(ctx, instanceName, route.ObjectPath)
	if err != nil {
		return false, time.Time{}, time.Time{}, false
	}
	defer reader.Close()

	info := reader.Info()
	fetchedAtStr := info.Options["fetched-at"]
	cachedAt, err := utils.ParseFetchedAt(fetchedAtStr)
	if err != nil {
		return false, time.Time{}, time.Time{}, false
	}

	expireAfter := route.ExpireAfter
	if expireAfter <= 0 {
		expireAfter = inst.ExpireAfter
	}
	var expiresAt time.Time
	if expireAfter > 0 {
		expiresAt = cachedAt.Add(expireAfter.Duration())
	}

	freshFor := route.FreshFor
	if freshFor <= 0 {
		freshFor = inst.Cache.FreshFor
	}
	fresh := false
	if freshFor > 0 {
		fresh = time.Since(cachedAt) <= freshFor.Duration()
	}

	return true, cachedAt, expiresAt, fresh
}
