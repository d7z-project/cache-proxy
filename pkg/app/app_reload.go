package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

const drainTimeout = 10 * time.Second

var ErrReloadInProgress = errors.New("config reload already in progress")

func (a *App) Reload(ctx context.Context) error {
	if !a.reloading.CompareAndSwap(false, true) {
		return ErrReloadInProgress
	}
	defer a.reloading.Store(false)
	a.lifecycleMu.Lock()
	defer a.lifecycleMu.Unlock()
	if a.closed.Load() {
		return errors.New("app is closed")
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	newDoc, err := config.LoadFile(a.configPath)
	if err != nil {
		return fmt.Errorf("reload: %w", err)
	}
	normalizeDocument(newDoc)
	if err := validateServerConfig(newDoc); err != nil {
		return fmt.Errorf("reload: %w", err)
	}
	if err := Validate(newDoc); err != nil {
		return fmt.Errorf("reload: %w", err)
	}

	added, removed, modified := config.DiffInstances(a.config.Instances, newDoc.Instances)
	if len(added) == 0 && len(removed) == 0 && len(modified) == 0 {
		a.downloads.Update(newDoc.Storage.Download.MaxActive, newDoc.Storage.Download.MaxActivePerInstance)
		a.routesMu.Lock()
		a.config = newDoc
		a.routesMu.Unlock()
		a.notifyStorageConfigChanged()
		return nil
	}
	slog.Info("reloading config", "added", len(added), "removed", len(removed), "modified", len(modified))

	// Phase 1: Plan all new instances (dry-run — no Start). If this fails, nothing changed.
	plan := proxyruntime.NewPlanContext(a.store, a.stats, a.downloads, newDoc.Server.Bind, newDoc.Metrics.Path)
	drivers := builtinDrivers()
	for _, inst := range newDoc.Instances {
		sel, err := inst.SelectMode()
		if err != nil {
			return err
		}
		drv, ok := drivers[sel.Mode]
		if !ok {
			return fmt.Errorf("instance %s: unsupported mode %q", inst.Name, sel.Mode)
		}
		ip, err := plan.Instance(inst, sel)
		if err != nil {
			return err
		}
		if err := drv.Plan(ctx, ip); err != nil {
			return err
		}
	}
	result, err := plan.Finalize()
	if err != nil {
		return err
	}
	plannedEntries := make(map[string]*proxyruntime.Entry, len(result.Entries))
	for _, entry := range result.Entries {
		if entry.ExpireAfter.IsUnset() {
			entry.ExpireAfter = config.DefaultExpireAfter
		}
		plannedEntries[entry.Name] = entry
	}

	addSet := setOf(added)
	modSet := setOf(modified)
	stopSet := setOf(append(removed, modified...))

	// Phase 2: Capture old entries. They keep serving until commit.
	a.routesMu.RLock()
	var oldEntriesToStop []*proxyruntime.Entry
	for _, name := range append(removed, modified...) {
		if entry := a.entries[name]; entry != nil {
			oldEntriesToStop = append(oldEntriesToStop, entry)
		}
	}
	a.routesMu.RUnlock()

	// Phase 3: Start new handlers for added + modified instances.
	var startedEntries []*proxyruntime.Entry
	for _, inst := range newDoc.Instances {
		entry, ok := plannedEntries[inst.Name]
		if !ok || !entry.Enabled || entry.Runtime == nil {
			continue
		}
		_, isAdd := addSet[inst.Name]
		_, isMod := modSet[inst.Name]
		if !isAdd && !isMod {
			continue
		}
		entryCtx, entryCancel := context.WithCancel(a.lifecycleCtx)
		entry.Ctx = entryCtx
		entry.Cancel = entryCancel
		if err := entry.Runtime.Start(entryCtx); err != nil {
			entryCancel()
			stopCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
			if stopErr := entry.Runtime.Stop(stopCtx); stopErr != nil {
				slog.Warn("failed instance cleanup after start error", "instance", entry.Name, "err", stopErr)
			}
			cancel()
			stopPreparedEntries(startedEntries)
			return fmt.Errorf("instance %s: start failed: %w", entry.Name, err)
		}
		startedEntries = append(startedEntries, entry)
	}

	// Phase 4: Build new routing tables.
	newEntries := make(map[string]*proxyruntime.Entry, len(a.entries))
	newPathHandlers := make(map[string]http.Handler, len(a.pathHandlers))
	newPathPrefixes := make([]string, 0, len(a.pathPrefixes))
	newBindHandlers := make(map[string]http.Handler, len(a.bindHandlers))
	var newHandlers []proxyruntime.Instance

	a.routesMu.RLock()
	for _, entry := range a.entries {
		if _, stop := stopSet[entry.Name]; stop {
			continue
		}
		newEntries[entry.Name] = entry
		newHandlers = append(newHandlers, entry.Runtime)
		if entry.Path != "" {
			newPathHandlers[entry.Path] = a.pathHandlers[entry.Path]
			newPathPrefixes = append(newPathPrefixes, entry.Path)
		} else {
			newBindHandlers[entry.Bind] = a.bindHandlers[entry.Bind]
		}
	}
	a.routesMu.RUnlock()

	for _, inst := range newDoc.Instances {
		entry, ok := plannedEntries[inst.Name]
		if !ok || !entry.Enabled || entry.Runtime == nil {
			continue
		}
		_, isAdd := addSet[inst.Name]
		_, isMod := modSet[inst.Name]
		if !isAdd && !isMod {
			continue
		}
		newEntries[entry.Name] = entry
		newHandlers = append(newHandlers, entry.Runtime)
		if entry.Path != "" {
			newPathHandlers[entry.Path] = entry.Runtime
			newPathPrefixes = append(newPathPrefixes, entry.Path)
		} else {
			newBindHandlers[entry.Bind] = bindHomeHandler{
				app:   a,
				entry: entry,
				next:  entry.Runtime,
			}
		}
	}
	sort.Slice(newPathPrefixes, func(i, j int) bool {
		if len(newPathPrefixes[i]) == len(newPathPrefixes[j]) {
			return newPathPrefixes[i] > newPathPrefixes[j]
		}
		return len(newPathPrefixes[i]) > len(newPathPrefixes[j])
	})

	// Phase 5: Start listeners for newly introduced bind addresses. Existing
	// bind servers use bindDispatchHandler and pick up the swapped handler.
	removedAddrs := make(map[string]struct{}, len(oldEntriesToStop))
	for _, e := range oldEntriesToStop {
		if e.Bind != "" {
			removedAddrs[e.Bind] = struct{}{}
		}
	}
	newBindServers := make(map[string]*http.Server, len(a.bindServers))
	newBindListeners := make(map[string]net.Listener, len(a.bindListeners))
	for addr := range a.bindServers {
		newBindServers[addr] = a.bindServers[addr]
	}
	for addr := range a.bindListeners {
		newBindListeners[addr] = a.bindListeners[addr]
	}
	var newStarted []string
	for addr := range newBindHandlers {
		if _, exists := newBindServers[addr]; exists {
			continue
		}
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			closePreparedBindServers(newStarted, newBindServers, newBindListeners)
			stopPreparedEntries(startedEntries)
			return fmt.Errorf("listen %s: %w", addr, err)
		}
		newBindListeners[addr] = listener
		srv := &http.Server{Addr: addr, Handler: bindDispatchHandler{app: a, addr: addr}}
		newBindServers[addr] = srv
		newStarted = append(newStarted, addr)
		go func(server *http.Server, listener net.Listener) {
			if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("bind server error", "addr", server.Addr, "err", err)
			}
		}(srv, listener)
	}

	// Phase 6: Atomic swap. Requests see either the old complete routing table
	// or the new complete routing table.
	a.ready.Store(false)
	a.routesMu.Lock()
	a.entries = newEntries
	a.handlers = newHandlers
	a.pathHandlers = newPathHandlers
	a.pathPrefixes = newPathPrefixes
	a.bindHandlers = newBindHandlers
	a.bindServers = newBindServers
	a.bindListeners = newBindListeners
	a.config = newDoc
	a.routesMu.Unlock()
	a.downloads.Update(newDoc.Storage.Download.MaxActive, newDoc.Storage.Download.MaxActivePerInstance)
	a.notifyStorageConfigChanged()
	a.ready.Store(true)

	// Phase 7: Stop bind servers that no remaining instance owns.
	for addr := range removedAddrs {
		if _, stillUsed := newBindHandlers[addr]; stillUsed {
			continue
		}
		a.routesMu.Lock()
		srv := a.bindServers[addr]
		listener := a.bindListeners[addr]
		delete(a.bindServers, addr)
		delete(a.bindListeners, addr)
		a.routesMu.Unlock()
		if srv != nil {
			if err := srv.Shutdown(ctx); err != nil {
				slog.Warn("bind server shutdown error", "addr", addr, "err", err)
			}
		}
		if listener != nil {
			_ = listener.Close()
		}
	}

	// Phase 8: Stop old handlers. Routes already swapped; no new requests can reach them.
	for _, entry := range oldEntriesToStop {
		if entry.Cancel != nil {
			entry.Cancel()
		}
		stopCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
		if err := entry.Runtime.Stop(stopCtx); err != nil {
			slog.Warn("instance stop timeout", "instance", entry.Name, "err", err)
		}
		cancel()
	}

	// Phase 9: Clean up removed tenants and stats. Modified instances keep the
	// same tenant so the freshly prepared runtime cannot have its state deleted.
	for _, name := range removed {
		if err := a.store.DeleteTenant(ctx, name); err != nil {
			slog.Warn("delete tenant failed", "tenant", name, "err", err)
		}
		a.stats.RemoveInstance(name)
	}

	// Phase 10: Finalize.
	saveRegistry(ctx, a.store, newDoc)
	slog.Info("config reload complete", "added", len(added), "removed", len(removed), "modified", len(modified))
	return nil
}

func stopPreparedEntries(entries []*proxyruntime.Entry) {
	for _, entry := range entries {
		if entry.Cancel != nil {
			entry.Cancel()
		}
		stopCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
		if err := entry.Runtime.Stop(stopCtx); err != nil {
			slog.Warn("prepared instance stop failed", "instance", entry.Name, "err", err)
		}
		cancel()
	}
}

func closePreparedBindServers(addrs []string, servers map[string]*http.Server, listeners map[string]net.Listener) {
	for _, addr := range addrs {
		if srv, ok := servers[addr]; ok {
			_ = srv.Shutdown(context.Background())
			delete(servers, addr)
		}
		if listener, ok := listeners[addr]; ok {
			_ = listener.Close()
			delete(listeners, addr)
		}
	}
}

func setOf(names []string) map[string]struct{} {
	m := make(map[string]struct{}, len(names))
	for _, n := range names {
		m[n] = struct{}{}
	}
	return m
}
