package server

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

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
	writeJSON(w, resetSystemResponse{Generation: nextGeneration}, nil)
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
	if freshFor.IsUnset() {
		freshFor = resolved.Driver.DefaultFreshFor(resolved)
	}
	if !freshFor.IsUnset() && !freshFor.IsForever() {
		result.FreshFor = freshFor.String()
	}
	expireAfter := route.ExpireAfter
	if expireAfter.IsUnset() {
		expireAfter = spec.Meta.ExpireAfter
	}
	if !expireAfter.IsUnset() && !expireAfter.IsNever() {
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

func (r *Runtime) checkCacheStatus(ctx context.Context, instanceName, objectPath string, freshFor config.Freshness, expireAfter config.Expiration) (bool, time.Time, time.Time, bool) {
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
	if !expireAfter.IsUnset() && !expireAfter.IsNever() {
		expiresAt = fetchedAt.Add(expireAfter.Duration())
	}
	fresh := false
	if !freshFor.IsUnset() && !freshFor.IsForever() {
		fresh = time.Since(fetchedAt) <= freshFor.Duration()
	}
	return true, fetchedAt, expiresAt, fresh
}

func (r *Runtime) storageCleanupAPI(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if !r.cleanupConfig().Enabled {
		http.Error(w, "cleanup is not enabled", http.StatusBadRequest)
		return
	}

	go r.runCleanup()
	w.WriteHeader(http.StatusAccepted)
}
