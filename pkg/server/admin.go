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
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

type globalConfigResponse struct {
	Generation uint64               `json:"generation"`
	Config     *config.GlobalConfig `json:"config"`
}

type instanceDocumentResponse struct {
	Generation uint64              `json:"generation"`
	Spec       config.InstanceSpec `json:"spec"`
}

type exportResponse struct {
	Generation uint64                `json:"generation"`
	Global     *config.GlobalConfig  `json:"global"`
	Instances  []config.InstanceSpec `json:"instances"`
}

type saveGlobalRequest struct {
	Generation uint64               `json:"generation"`
	Config     *config.GlobalConfig `json:"config"`
}

type saveInstanceRequest struct {
	Generation uint64               `json:"generation"`
	Spec       *config.InstanceSpec `json:"spec"`
}

type importInstancesRequest struct {
	Generation uint64                `json:"generation"`
	Replace    bool                  `json:"replace"`
	Instances  []config.InstanceSpec `json:"instances"`
}

func (r *Runtime) serveAPI(w http.ResponseWriter, req *http.Request) {
	switch {
	case req.URL.Path == "/-/api/runtime":
		r.runtimeAPI(w, req)
	case req.URL.Path == "/-/api/global-config":
		r.globalConfigAPI(w, req)
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
	case req.URL.Path == "/-/api/cache/lookup":
		r.cacheLookupAPI(w, req)
	case req.URL.Path == "/-/api/system/reset":
		r.resetAPI(w, req)
	default:
		http.NotFound(w, req)
	}
}

func (r *Runtime) publicInstancesAPI(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	r.mu.RLock()
	instances := config.CloneInstances(r.instances)
	r.mu.RUnlock()
	writeJSON(w, listInstanceSummaries(instances), nil)
}

func (r *Runtime) runtimeAPI(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	stats := r.stats.Snapshot()
	r.mu.RLock()
	response := map[string]any{
		"bind":        r.bind,
		"backend":     r.backend,
		"authEnabled": r.password != "",
		"metricsPath": r.global.Metrics.Path,
		"gcInterval":  r.global.Storage.GC.Blob.String(),
		"generation":  r.generation,
		"instances":   len(r.instances),
		"handlers":    len(r.handlers),
		"requests":    stats.Total.Requests,
		"errors":      stats.Total.Errors,
		"upstreams":   stats.Total.UpstreamRequests,
	}
	r.mu.RUnlock()
	writeJSON(w, response, nil)
}

func (r *Runtime) globalConfigAPI(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		r.mu.RLock()
		response := globalConfigResponse{Generation: r.generation, Config: config.CloneGlobal(r.global)}
		r.mu.RUnlock()
		writeJSON(w, response, nil)
	case http.MethodPut:
		var input saveGlobalRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, req.Body, int64(defaultConfigLimit))).Decode(&input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if input.Config == nil {
			writeError(w, http.StatusBadRequest, errors.New("config is required"))
			return
		}
		r.mu.RLock()
		instances := config.CloneInstances(r.instances)
		r.mu.RUnlock()
		nextGeneration, err := r.replaceState(req.Context(), input.Generation, input.Config, instances, nil, nil)
		if err != nil {
			writeConfigError(w, err)
			return
		}
		writeJSON(w, globalConfigResponse{Generation: nextGeneration, Config: config.CloneGlobal(input.Config)}, nil)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (r *Runtime) instancesCollectionAPI(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		r.mu.RLock()
		instances := config.CloneInstances(r.instances)
		generation := r.generation
		r.mu.RUnlock()
		writeJSON(w, map[string]any{"generation": generation, "items": listInstanceSummaries(instances)}, nil)
	case http.MethodPost:
		var input saveInstanceRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, req.Body, int64(defaultConfigLimit))).Decode(&input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if input.Spec == nil {
			writeError(w, http.StatusBadRequest, errors.New("spec is required"))
			return
		}
		spec := config.CloneInstance(*input.Spec)
		r.mu.RLock()
		global := config.CloneGlobal(r.global)
		instances := config.CloneInstances(r.instances)
		r.mu.RUnlock()
		if _, exists := instances[spec.Name]; exists {
			writeError(w, http.StatusConflict, errors.New("instance already exists"))
			return
		}
		instances[spec.Name] = spec
		nextGeneration, err := r.replaceState(req.Context(), input.Generation, global, instances, []config.InstanceSpec{spec}, nil)
		if err != nil {
			writeConfigError(w, err)
			return
		}
		writeJSON(w, instanceDocumentResponse{Generation: nextGeneration, Spec: spec}, nil)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (r *Runtime) instanceDocumentAPI(w http.ResponseWriter, req *http.Request, name string) {
	if name == "" || strings.Contains(name, "/") {
		http.NotFound(w, req)
		return
	}
	switch req.Method {
	case http.MethodGet:
		r.mu.RLock()
		spec, ok := r.instances[name]
		generation := r.generation
		r.mu.RUnlock()
		if !ok {
			writeError(w, http.StatusNotFound, errors.New("instance not found"))
			return
		}
		writeJSON(w, instanceDocumentResponse{Generation: generation, Spec: config.CloneInstance(spec)}, nil)
	case http.MethodPut:
		var input saveInstanceRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, req.Body, int64(defaultConfigLimit))).Decode(&input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if input.Spec == nil {
			writeError(w, http.StatusBadRequest, errors.New("spec is required"))
			return
		}
		spec := config.CloneInstance(*input.Spec)
		if spec.Name != name {
			writeError(w, http.StatusBadRequest, errors.New("instance name cannot be changed"))
			return
		}
		r.mu.RLock()
		global := config.CloneGlobal(r.global)
		instances := config.CloneInstances(r.instances)
		r.mu.RUnlock()
		if _, exists := instances[name]; !exists {
			writeError(w, http.StatusNotFound, errors.New("instance not found"))
			return
		}
		instances[name] = spec
		nextGeneration, err := r.replaceState(req.Context(), input.Generation, global, instances, []config.InstanceSpec{spec}, nil)
		if err != nil {
			writeConfigError(w, err)
			return
		}
		writeJSON(w, instanceDocumentResponse{Generation: nextGeneration, Spec: spec}, nil)
	case http.MethodDelete:
		generation, err := parseGeneration(req)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		r.mu.RLock()
		global := config.CloneGlobal(r.global)
		instances := config.CloneInstances(r.instances)
		r.mu.RUnlock()
		if _, exists := instances[name]; !exists {
			writeError(w, http.StatusNotFound, errors.New("instance not found"))
			return
		}
		delete(instances, name)
		nextGeneration, replaceErr := r.replaceState(req.Context(), generation, global, instances, nil, []string{name})
		if replaceErr != nil {
			writeConfigError(w, replaceErr)
			return
		}
		writeJSON(w, map[string]any{"generation": nextGeneration, "deleted": name}, nil)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (r *Runtime) instancesExportAPI(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	r.mu.RLock()
	items := make([]config.InstanceSpec, 0, len(r.instances))
	for _, name := range sortedInstanceNames(r.instances) {
		items = append(items, config.CloneInstance(r.instances[name]))
	}
	response := exportResponse{
		Generation: r.generation,
		Global:     config.CloneGlobal(r.global),
		Instances:  items,
	}
	r.mu.RUnlock()
	writeJSON(w, response, nil)
}

func (r *Runtime) instancesImportAPI(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var input importInstancesRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, req.Body, int64(defaultConfigLimit))).Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if len(input.Instances) == 0 {
		writeError(w, http.StatusBadRequest, errors.New("no instances to import"))
		return
	}
	r.mu.RLock()
	global := config.CloneGlobal(r.global)
	instances := config.CloneInstances(r.instances)
	r.mu.RUnlock()
	changed := make([]config.InstanceSpec, 0, len(input.Instances))
	for _, item := range input.Instances {
		spec := config.CloneInstance(item)
		if spec.Name == "" {
			writeError(w, http.StatusBadRequest, errors.New("instance name is required"))
			return
		}
		if _, exists := instances[spec.Name]; exists && !input.Replace {
			writeError(w, http.StatusConflict, errors.New("instance already exists: "+spec.Name))
			return
		}
		instances[spec.Name] = spec
		changed = append(changed, spec)
	}
	nextGeneration, err := r.replaceState(req.Context(), input.Generation, global, instances, changed, nil)
	if err != nil {
		writeConfigError(w, err)
		return
	}
	writeJSON(w, map[string]any{"generation": nextGeneration, "imported": len(changed)}, nil)
}

func (r *Runtime) resetAPI(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	r.mu.RLock()
	generation := r.generation
	global := DefaultGlobalConfig(r.global.Metrics.Path, r.global.Storage.GC.Blob.Duration())
	r.mu.RUnlock()
	instances := DefaultInstances()
	removed := []string{}
	r.mu.RLock()
	for name := range r.instances {
		if _, keep := instances[name]; !keep {
			removed = append(removed, name)
		}
	}
	r.mu.RUnlock()
	nextGeneration, err := r.replaceState(req.Context(), generation, global, instances, collectChanged(instances), removed)
	if err != nil {
		writeConfigError(w, err)
		return
	}
	writeJSON(w, map[string]any{"generation": nextGeneration}, nil)
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

type cacheLookupResult struct {
	Instance    string `json:"instance"`
	Mode        string `json:"mode"`
	Policy      string `json:"policy"`
	FreshFor    string `json:"freshFor,omitempty"`
	ExpireAfter string `json:"expireAfter,omitempty"`
	Generation  uint64 `json:"generation"`
	Cached      bool   `json:"cached"`
	CachedAt    string `json:"cachedAt,omitempty"`
	ExpiresAt   string `json:"expiresAt,omitempty"`
	Fresh       bool   `json:"fresh"`

	objectPath string
}

func (r *Runtime) cacheLookupAPI(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	instanceName := strings.TrimSpace(req.URL.Query().Get("instance"))
	lookupPath := strings.TrimSpace(req.URL.Query().Get("path"))
	if instanceName == "" || lookupPath == "" {
		writeError(w, http.StatusBadRequest, errors.New("instance and path are required"))
		return
	}
	r.mu.RLock()
	spec, ok := r.instances[instanceName]
	generation := r.generation
	r.mu.RUnlock()
	if !ok {
		writeError(w, http.StatusNotFound, errors.New("instance not found"))
		return
	}
	_, resolved, err := validateInstanceSpec(spec, r.registry)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	route, err := resolved.Driver.Lookup(resolved, lookupPath)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	result := cacheLookupResult{
		Instance:   instanceName,
		Mode:       spec.Meta.Mode,
		Policy:     route.Policy,
		Generation: generation,
		objectPath: route.ObjectPath,
	}
	freshFor := route.FreshFor
	if freshFor <= 0 {
		freshFor = resolved.Driver.DefaultFreshFor(resolved)
	}
	if freshFor > 0 {
		result.FreshFor = freshFor.String()
	}
	expireAfter := route.ExpireAfter
	if expireAfter <= 0 {
		expireAfter = spec.Meta.ExpireAfter
	}
	if expireAfter > 0 {
		result.ExpireAfter = expireAfter.String()
	}
	cached, cachedAt, expiresAt, fresh := r.checkCacheStatus(req.Context(), instanceName, result.objectPath, freshFor, expireAfter)
	result.Cached = cached
	result.Fresh = fresh
	if !cachedAt.IsZero() {
		result.CachedAt = cachedAt.UTC().Format(time.RFC3339)
	}
	if !expiresAt.IsZero() {
		result.ExpiresAt = expiresAt.UTC().Format(time.RFC3339)
	}
	writeJSON(w, result, nil)
}

func (r *Runtime) checkCacheStatus(ctx context.Context, instanceName, objectPath string, freshFor, expireAfter config.Duration) (bool, time.Time, time.Time, bool) {
	reader, err := r.store.OpenObject(ctx, instanceName, objectPath)
	if err != nil {
		return false, time.Time{}, time.Time{}, false
	}
	defer reader.Close()
	info := reader.Info()
	fetchedAt, err := utils.ParseFetchedAt(info.Options["fetched-at"])
	if err != nil {
		return false, time.Time{}, time.Time{}, false
	}
	var expiresAt time.Time
	if expireAfter > 0 {
		expiresAt = fetchedAt.Add(expireAfter.Duration())
	}
	fresh := false
	if freshFor > 0 {
		fresh = time.Since(fetchedAt) <= freshFor.Duration()
	}
	return true, fetchedAt, expiresAt, fresh
}
