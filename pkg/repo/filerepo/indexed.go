package filerepo

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"gopkg.d7z.net/blobfs"

	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

const (
	DefaultMaxObject  = int64(2 << 30)
	refreshInterval   = 15 * time.Minute
	gcInterval        = 6 * time.Hour
	continuationDelay = 2 * time.Second
	retryDelay        = time.Minute
)

type ObjectState string

const (
	ObjectPresent   ObjectState = "present"
	ObjectNotFound  ObjectState = "not_found"
	ObjectForbidden ObjectState = "forbidden"
)

type Checksum struct {
	Algorithm string
	Digest    string
}

type ObjectSpec struct {
	Path         string
	FetchPath    string
	ExpectedSize *int64
	MaxBytes     int64
	Checksums    []Checksum
	Optional     bool
}

type Object struct {
	Path   string      `json:"path"`
	State  ObjectState `json:"state"`
	Key    string      `json:"key,omitempty"`
	Size   int64       `json:"size,omitempty"`
	SHA256 string      `json:"sha256,omitempty"`
	Header http.Header `json:"header,omitempty"`
}

type Snapshot struct {
	RootID      string    `json:"root_id"`
	Root        string    `json:"root"`
	Anchor      string    `json:"anchor"`
	Generation  string    `json:"generation"`
	CandidateID string    `json:"candidate_id"`
	Upstream    string    `json:"upstream"`
	PublishedAt time.Time `json:"published_at"`
	Objects     []Object  `json:"objects"`

	byPath map[string]Object
}

type Anchor struct {
	RootID     string
	Root       string
	Path       string
	Upstream   string
	Generation string
	Header     http.Header
	blob       *Blob
}

type FetchFunc func(context.Context, string, string, http.Header) (*http.Response, error)
type BuildFunc func(context.Context, *RefreshSession, Anchor) error

type Config struct {
	Instance       string
	Mode           string
	Tenant         string
	StateDir       string
	WorkDir        string
	Spooler        *storeio.Spooler
	AnchorMaxBytes int64
	Store          *blobfs.Store
	Scheduler      *scheduler.Scheduler
	Fetch          FetchFunc
	Build          BuildFunc
	KeepPrevious   int
	GracePeriod    time.Duration
	InactiveAfter  time.Duration
}

type currentMarker struct {
	RootID         string `yaml:"root_id"`
	Root           string `yaml:"root"`
	Generation     string `yaml:"generation"`
	CandidateID    string `yaml:"candidate_id"`
	SnapshotSHA256 string `yaml:"snapshot_sha256"`
	Upstream       string `yaml:"upstream"`
}

type pendingAnchor struct {
	RootID      string      `json:"root_id"`
	Root        string      `json:"root"`
	Path        string      `json:"path"`
	Upstream    string      `json:"upstream"`
	Generation  string      `json:"generation"`
	CandidateID string      `json:"candidate_id"`
	Header      http.Header `json:"header"`
	Key         string      `json:"key"`
}

type lastSeenMarker struct {
	RootID string    `json:"root_id"`
	SeenAt time.Time `json:"seen_at"`
}

type liveSnapshot struct {
	snapshot *Snapshot
}

type gcCandidate struct {
	name     string
	modified time.Time
	prefix   string
	snapshot Snapshot
	keys     []string
}

type generationGCPhase uint8

const (
	generationGCIdle generationGCPhase = iota
	generationGCRetire
	generationGCScan
	generationGCDelete
	generationGCBlobs
)

type retryWindow struct {
	candidateID string
	notBefore   time.Time
}

type GenerationManager struct {
	config            Config
	mu                sync.RWMutex
	current           map[string]*liveSnapshot
	pending           map[string]pendingAnchor
	readers           map[string]int
	lastSeen          map[string]time.Time
	lastSeenPersisted map[string]time.Time
	retryWindows      map[string]retryWindow
	retiring          map[string]bool
	commitMu          sync.Mutex
	seenPersistMu     sync.Mutex
	discoveryMu       sync.Mutex
	discoveryPending  map[string]bool
	refreshMu         sync.Mutex
	refreshCursor     string
	pollCursor        string
	pollNext          bool
	gcMu              sync.Mutex
	gcPhase           generationGCPhase
	gcCursor          string
	gcCandidates      []gcCandidate
	gcCandidateIndex  int
	gcObjectIndex     int
	gcRetained        map[string]bool
	gcProtected       map[string]bool
}

type retryableRefreshError struct {
	err error
}

func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return &retryableRefreshError{err: err}
}

func (e *retryableRefreshError) Error() string { return e.err.Error() }
func (e *retryableRefreshError) Unwrap() error { return e.err }

func New(config Config) (*GenerationManager, error) {
	if config.Store == nil || config.Fetch == nil || config.Build == nil || config.StateDir == "" || config.Tenant == "" {
		return nil, errors.New("filerepo configuration is incomplete")
	}
	if config.KeepPrevious < 1 {
		config.KeepPrevious = 1
	}
	if config.GracePeriod <= 0 {
		config.GracePeriod = time.Hour
	}
	if config.InactiveAfter <= 0 {
		config.InactiveAfter = 30 * 24 * time.Hour
	}
	if config.Spooler == nil {
		config.Spooler = storeio.NewSpooler(config.WorkDir, DefaultMaxObject, nil)
	}
	if config.AnchorMaxBytes <= 0 {
		config.AnchorMaxBytes = DefaultMaxObject
	}
	h := &GenerationManager{
		config: config, current: make(map[string]*liveSnapshot), pending: make(map[string]pendingAnchor), readers: make(map[string]int),
		lastSeen: make(map[string]time.Time), lastSeenPersisted: make(map[string]time.Time), retryWindows: make(map[string]retryWindow), retiring: make(map[string]bool),
		discoveryPending: make(map[string]bool),
	}
	if err := h.restore(); err != nil {
		return nil, err
	}
	if config.Scheduler != nil {
		config.Scheduler.Register(scheduler.TaskDef{
			Key:      scheduler.NewTaskKey(config.Instance, scheduler.TypeMetadataRefresh, config.Mode),
			Interval: refreshInterval,
			Handler: func(ctx context.Context) (*scheduler.TaskOutcome, error) {
				more, err := h.Refresh(ctx, 1)
				outcome := &scheduler.TaskOutcome{Result: "success"}
				if err != nil {
					outcome.ContinueAfter = retryDelay
				} else if more {
					outcome.ContinueAfter = continuationDelay
				} else {
					now := time.Now()
					h.mu.RLock()
					for rootID, retry := range h.retryWindows {
						pending := h.pending[rootID]
						current := h.current[rootID]
						active := pending.CandidateID == retry.candidateID || current != nil && current.snapshot.CandidateID == retry.candidateID
						if wait := retry.notBefore.Sub(now); active && wait > 0 && (outcome.ContinueAfter == 0 || wait < outcome.ContinueAfter) {
							outcome.ContinueAfter = wait
						}
					}
					h.mu.RUnlock()
				}
				return outcome, err
			},
		})
		config.Scheduler.Register(scheduler.TaskDef{
			Key:      scheduler.NewTaskKey(config.Instance, scheduler.TypeMetadataGC, config.Mode),
			Interval: gcInterval,
			Handler: func(ctx context.Context) (*scheduler.TaskOutcome, error) {
				more, err := h.GC(ctx, 100)
				outcome := &scheduler.TaskOutcome{Result: "success"}
				if more {
					outcome.ContinueAfter = continuationDelay
				}
				return outcome, err
			},
		})
	}
	return h, nil
}

func (h *GenerationManager) StageAnchor(ctx context.Context, root, anchorPath, upstream string, header http.Header, body io.ReadSeeker) error {
	return h.StageAnchorID(ctx, root, root, anchorPath, upstream, header, body)
}

func (h *GenerationManager) StageAnchorID(ctx context.Context, rootID, root, anchorPath, upstream string, header http.Header, body io.ReadSeeker) error {
	root, err := cleanRoot(root)
	if err != nil {
		return err
	}
	anchorPath, err = CleanPath(anchorPath)
	if err != nil {
		return err
	}
	if _, err := body.Seek(0, io.SeekStart); err != nil {
		return err
	}
	digest := sha256.New()
	readLimit := h.config.AnchorMaxBytes
	if readLimit < math.MaxInt64 {
		readLimit++
	}
	size, err := io.Copy(digest, io.LimitReader(body, readLimit))
	if err != nil {
		return err
	}
	if size > h.config.AnchorMaxBytes {
		return storeio.ErrObjectTooLarge
	}
	generation := hex.EncodeToString(digest.Sum(nil))
	candidateBytes := make([]byte, 16)
	if _, err := rand.Read(candidateBytes); err != nil {
		return fmt.Errorf("create candidate ID: %w", err)
	}
	candidateID := hex.EncodeToString(candidateBytes)
	if strings.TrimSpace(rootID) == "" {
		return errors.New("metadata root ID is empty")
	}
	key := candidatePrefix(rootID, generation, candidateID) + "/anchor"
	if err := h.config.Store.MkdirAll(h.config.Tenant+"/"+path.Dir(key), 0o755); err != nil {
		return err
	}
	if _, err := body.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := h.config.Store.Put(ctx, h.config.Tenant, key, body, nil); err != nil {
		return fmt.Errorf("stage metadata anchor: %w", err)
	}
	pending := pendingAnchor{RootID: rootID, Root: root, Path: anchorPath, Upstream: upstream, Generation: generation, CandidateID: candidateID, Header: cloneHeader(header), Key: key}
	now := time.Now().UTC()
	preparedLastSeen, err := prepareJSON(h.config.StateDir, lastSeenName(rootID), lastSeenMarker{RootID: rootID, SeenAt: now}, maxRepositoryMarkerSize)
	if err != nil {
		return err
	}
	defer preparedLastSeen.discard()
	preparedPending, err := prepareJSON(h.config.StateDir, pendingName(rootID), pending, maxRepositoryMarkerSize)
	if err != nil {
		return err
	}
	defer preparedPending.discard()
	h.commitMu.Lock()
	defer h.commitMu.Unlock()
	if err := preparedLastSeen.commit(); err != nil {
		return err
	}
	if err := preparedPending.commit(); err != nil {
		return err
	}
	h.mu.Lock()
	h.pending[rootID] = pending
	h.lastSeen[rootID] = now
	h.lastSeenPersisted[rootID] = now
	delete(h.retryWindows, rootID)
	h.mu.Unlock()
	h.TriggerRefresh()
	return nil
}

func (h *GenerationManager) TriggerRefresh() {
	if h.config.Scheduler != nil {
		h.config.Scheduler.TriggerNow(scheduler.NewTaskKey(h.config.Instance, scheduler.TypeMetadataRefresh, h.config.Mode))
	}
}

func (h *GenerationManager) Discover(ctx context.Context, rootID, root, anchorPath, upstream string) error {
	response, err := h.config.Fetch(ctx, upstream, anchorPath, http.Header{"Accept-Encoding": {"identity"}})
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("metadata discovery anchor %s returned %d", anchorPath, response.StatusCode)
	}
	if encoding := response.Header.Get("Content-Encoding"); encoding != "" && !strings.EqualFold(encoding, "identity") {
		return fmt.Errorf("metadata discovery anchor %s returned content encoding %q", anchorPath, encoding)
	}
	spool, err := h.config.Spooler.SpoolWithExpectedSize(ctx, response.Body, h.config.AnchorMaxBytes, response.ContentLength)
	if err != nil {
		return err
	}
	defer func() { _ = spool.Close() }()
	return h.StageAnchorID(ctx, rootID, root, anchorPath, upstream, response.Header, spool.File)
}

func (h *GenerationManager) ScheduleDiscovery(lifecycle *storeio.Lifecycle, rootID, root, anchorPath, upstream string) {
	if lifecycle == nil {
		return
	}
	key := rootID + "\x00" + upstream + "\x00" + anchorPath
	h.discoveryMu.Lock()
	if h.discoveryPending[key] {
		h.discoveryMu.Unlock()
		return
	}
	h.discoveryPending[key] = true
	h.discoveryMu.Unlock()
	err := lifecycle.Go(func(ctx context.Context) {
		defer func() {
			h.discoveryMu.Lock()
			delete(h.discoveryPending, key)
			h.discoveryMu.Unlock()
		}()
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if err := h.Discover(ctx, rootID, root, anchorPath, upstream); err != nil {
			slog.Warn("metadata background discovery failed", "mode", h.config.Mode, "path", anchorPath, "err", err)
		}
	})
	if err != nil {
		h.discoveryMu.Lock()
		delete(h.discoveryPending, key)
		h.discoveryMu.Unlock()
	}
}

func (h *GenerationManager) Refresh(ctx context.Context, limit int) (bool, error) {
	h.refreshMu.Lock()
	defer h.refreshMu.Unlock()
	now := time.Now()
	h.mu.RLock()
	roots := make([]string, 0, len(h.pending))
	for rootID, pending := range h.pending {
		retry := h.retryWindows[rootID]
		if retry.candidateID != pending.CandidateID || !now.Before(retry.notBefore) {
			roots = append(roots, rootID)
		}
	}
	pendingCount := len(h.pending)
	h.mu.RUnlock()
	hadPending := pendingCount != 0
	if !hadPending || h.pollNext {
		if err := h.pollCurrent(ctx); err != nil {
			h.pollNext = false
			if !hadPending {
				return false, err
			}
			slog.Warn("metadata poll failed while pending refresh remains runnable", "mode", h.config.Mode, "err", err)
		}
		h.pollNext = false
		if !hadPending {
			h.mu.RLock()
			for rootID, pending := range h.pending {
				retry := h.retryWindows[rootID]
				if retry.candidateID != pending.CandidateID || !now.Before(retry.notBefore) {
					roots = append(roots, rootID)
				}
			}
			h.mu.RUnlock()
		}
	}
	sort.Strings(roots)
	if h.refreshCursor != "" && len(roots) > 1 {
		start := sort.SearchStrings(roots, h.refreshCursor)
		for start < len(roots) && roots[start] <= h.refreshCursor {
			start++
		}
		if start == len(roots) {
			start = 0
		}
		roots = append(roots[start:], roots[:start]...)
	}
	if limit <= 0 {
		limit = len(roots)
	}
	processed := 0
	for _, rootID := range roots {
		if processed >= limit {
			return true, nil
		}
		h.mu.RLock()
		pending := h.pending[rootID]
		retry := h.retryWindows[rootID]
		blocked := retry.candidateID == pending.CandidateID && time.Now().Before(retry.notBefore)
		h.mu.RUnlock()
		if blocked {
			continue
		}
		h.refreshCursor = rootID
		h.pollNext = true
		if err := h.refreshRoot(ctx, rootID); err != nil {
			h.mu.Lock()
			if latest, exists := h.pending[rootID]; exists && latest.CandidateID == pending.CandidateID {
				h.retryWindows[rootID] = retryWindow{candidateID: pending.CandidateID, notBefore: time.Now().Add(retryDelay)}
			}
			h.mu.Unlock()
			return len(roots) > 1, err
		}
		h.mu.Lock()
		delete(h.retryWindows, rootID)
		h.mu.Unlock()
		processed++
	}
	return false, nil
}

func (h *GenerationManager) pollCurrent(ctx context.Context) error {
	h.mu.RLock()
	snapshots := make([]*Snapshot, 0, len(h.current))
	for _, current := range h.current {
		snapshots = append(snapshots, current.snapshot)
	}
	h.mu.RUnlock()
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].RootID < snapshots[j].RootID })
	if len(snapshots) > 1 && h.pollCursor != "" {
		start := sort.Search(len(snapshots), func(i int) bool { return snapshots[i].RootID > h.pollCursor })
		if start == len(snapshots) {
			start = 0
		}
		snapshots = append(snapshots[start:], snapshots[:start]...)
	}
	for _, snapshot := range snapshots {
		h.pollCursor = snapshot.RootID
		h.mu.RLock()
		retry := h.retryWindows[snapshot.RootID]
		blocked := retry.candidateID == snapshot.CandidateID && time.Now().Before(retry.notBefore)
		h.mu.RUnlock()
		if blocked {
			continue
		}
		validators := make(http.Header)
		if anchor, ok := snapshot.byPath[snapshot.Anchor]; ok {
			if value := anchor.Header.Get("ETag"); value != "" {
				validators.Set("If-None-Match", value)
			}
			if value := anchor.Header.Get("Last-Modified"); value != "" {
				validators.Set("If-Modified-Since", value)
			}
		}
		response, err := h.config.Fetch(ctx, snapshot.Upstream, snapshot.Anchor, validators)
		if err != nil {
			h.recordCurrentPollResult(snapshot, err)
			return err
		}
		if response.StatusCode == http.StatusNotModified {
			_ = response.Body.Close()
			err := h.updateCurrentFreshness(snapshot.RootID, snapshot.CandidateID, response.Header)
			h.recordCurrentPollResult(snapshot, err)
			return err
		}
		if response.StatusCode != http.StatusOK {
			_ = response.Body.Close()
			err := fmt.Errorf("metadata anchor %s returned %d", snapshot.Anchor, response.StatusCode)
			h.recordCurrentPollResult(snapshot, err)
			return err
		}
		spool, err := h.config.Spooler.SpoolWithExpectedSize(ctx, response.Body, h.config.AnchorMaxBytes, response.ContentLength)
		_ = response.Body.Close()
		if err != nil {
			h.recordCurrentPollResult(snapshot, err)
			return err
		}
		if spool.SHA256 == snapshot.Generation {
			_ = spool.Close()
			err := h.updateCurrentFreshness(snapshot.RootID, snapshot.CandidateID, response.Header)
			h.recordCurrentPollResult(snapshot, err)
			return err
		}
		err = h.StageAnchorID(ctx, snapshot.RootID, snapshot.Root, snapshot.Anchor, snapshot.Upstream, response.Header, spool.File)
		_ = spool.Close()
		h.recordCurrentPollResult(snapshot, err)
		return err
	}
	return nil
}

func (h *GenerationManager) recordCurrentPollResult(snapshot *Snapshot, pollErr error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if pollErr == nil {
		delete(h.retryWindows, snapshot.RootID)
		return
	}
	if current := h.current[snapshot.RootID]; current != nil && current.snapshot.CandidateID == snapshot.CandidateID {
		h.retryWindows[snapshot.RootID] = retryWindow{candidateID: snapshot.CandidateID, notBefore: time.Now().Add(retryDelay)}
	}
}

func (h *GenerationManager) updateCurrentFreshness(rootID, candidateID string, header http.Header) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	live := h.current[rootID]
	if live == nil || live.snapshot.CandidateID != candidateID {
		return nil
	}
	updated := *live.snapshot
	updated.Objects = append([]Object(nil), live.snapshot.Objects...)
	updated.PublishedAt = time.Now().UTC()
	for index := range updated.Objects {
		if updated.Objects[index].Path != updated.Anchor {
			continue
		}
		updated.Objects[index].Header = cloneHeader(updated.Objects[index].Header)
		for _, name := range []string{"Cache-Control", "Content-Location", "ETag", "Expires", "Last-Modified", "Vary"} {
			if values := header.Values(name); len(values) != 0 {
				updated.Objects[index].Header[http.CanonicalHeaderKey(name)] = append([]string(nil), values...)
			}
		}
	}
	if err := prepareSnapshot(&updated); err != nil {
		return err
	}
	// Candidate snapshots are immutable. Persisting this update would require
	// replacing both the snapshot and current marker and creates a crash window
	// where their digests disagree. Losing freshness on restart only causes an
	// early same-origin conditional poll.
	h.current[rootID] = &liveSnapshot{snapshot: &updated}
	return nil
}

func (h *GenerationManager) refreshRoot(ctx context.Context, rootID string) error {
	h.mu.RLock()
	pending, ok := h.pending[rootID]
	h.mu.RUnlock()
	if !ok {
		return nil
	}
	anchorReader, err := h.config.Store.OpenObject(ctx, h.config.Tenant, pending.Key)
	if err != nil {
		return fmt.Errorf("open staged anchor: %w", err)
	}
	anchorBlob := &Blob{handler: h, object: Object{Path: pending.Path, State: ObjectPresent, Key: pending.Key, Size: anchorReader.Info().Size, SHA256: pending.Generation, Header: pending.Header}}
	_ = anchorReader.Close()
	session := &RefreshSession{handler: h, rootID: rootID, root: pending.Root, upstream: pending.Upstream, generation: pending.Generation, candidateID: pending.CandidateID, objects: make(map[string]Object)}
	session.objects[pending.Path] = anchorBlob.object
	anchor := Anchor{RootID: rootID, Root: pending.Root, Path: pending.Path, Upstream: pending.Upstream, Generation: pending.Generation, Header: pending.Header, blob: anchorBlob}
	if err := h.config.Build(ctx, session, anchor); err != nil {
		var retryableError *retryableRefreshError
		if !errors.As(err, &retryableError) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			h.discardCandidate(rootID, pending)
		}
		return err
	}
	objects := make([]Object, 0, len(session.objects))
	for _, object := range session.objects {
		objects = append(objects, object)
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Path < objects[j].Path })
	snapshot := &Snapshot{RootID: rootID, Root: pending.Root, Anchor: pending.Path, Generation: pending.Generation, CandidateID: pending.CandidateID, Upstream: pending.Upstream, PublishedAt: time.Now().UTC(), Objects: objects}
	if err := prepareSnapshot(snapshot); err != nil {
		return err
	}
	encoded, digest, err := encodeJSONDigest(snapshot)
	if err != nil {
		return err
	}
	if int64(len(encoded)) > maxSnapshotStateSize {
		return fmt.Errorf("metadata snapshot exceeds %d bytes", maxSnapshotStateSize)
	}
	if err := writeBytes(h.config.StateDir, snapshotName(rootID, pending.Generation, pending.CandidateID), encoded); err != nil {
		return err
	}
	marker := currentMarker{RootID: rootID, Root: pending.Root, Generation: pending.Generation, CandidateID: pending.CandidateID, SnapshotSHA256: digest, Upstream: pending.Upstream}
	preparedMarker, err := prepareYAML(h.config.StateDir, currentName(rootID), marker)
	if err != nil {
		return err
	}
	defer preparedMarker.discard()

	h.commitMu.Lock()
	h.mu.RLock()
	latest, stillPending := h.pending[rootID]
	sameCandidate := stillPending && latest.CandidateID == pending.CandidateID
	h.mu.RUnlock()
	if !sameCandidate {
		h.commitMu.Unlock()
		h.discardCandidate(rootID, pending)
		return errors.New("metadata anchor changed during refresh")
	}
	if err := preparedMarker.commit(); err != nil {
		h.commitMu.Unlock()
		return err
	}
	h.mu.Lock()
	h.current[rootID] = &liveSnapshot{snapshot: snapshot}
	delete(h.pending, rootID)
	h.mu.Unlock()
	_ = os.Remove(statePath(h.config.StateDir, pendingName(rootID)))
	h.commitMu.Unlock()
	return nil
}

func (h *GenerationManager) discardCandidate(rootID string, pending pendingAnchor) {
	h.commitMu.Lock()
	defer h.commitMu.Unlock()
	h.mu.Lock()
	latest, exists := h.pending[rootID]
	sameCandidate := exists && latest.CandidateID == pending.CandidateID
	if sameCandidate {
		delete(h.pending, rootID)
	}
	h.mu.Unlock()
	if sameCandidate {
		_ = os.Remove(statePath(h.config.StateDir, pendingName(rootID)))
	}
}

func (h *GenerationManager) ServeCurrent(w http.ResponseWriter, request *http.Request, requestPath string, classifiedMetadata bool) (bool, int, string) {
	return h.serveCurrent(w, request, requestPath, classifiedMetadata, "")
}

func (h *GenerationManager) ServeCurrentFor(w http.ResponseWriter, request *http.Request, requestPath string, classifiedMetadata bool, rootID string) (bool, int, string) {
	return h.serveCurrent(w, request, requestPath, classifiedMetadata, rootID)
}

func (h *GenerationManager) ServeStagedAnchorFor(w http.ResponseWriter, request *http.Request, requestPath, rootID string) bool {
	requestPath, err := CleanPath(requestPath)
	if err != nil {
		return false
	}
	h.mu.RLock()
	pending, ok := h.pending[rootID]
	h.mu.RUnlock()
	if !ok || pending.Path != requestPath {
		return false
	}
	reader, err := h.config.Store.OpenObject(request.Context(), h.config.Tenant, pending.Key)
	if err != nil {
		return false
	}
	defer func() { _ = reader.Close() }()
	copyHeaders(w.Header(), pending.Header)
	w.Header().Set("X-Cache", "COALESCED")
	http.ServeContent(w, request, path.Base(requestPath), time.Time{}, reader)
	return true
}

func (h *GenerationManager) serveCurrent(w http.ResponseWriter, request *http.Request, requestPath string, classifiedMetadata bool, rootID string) (bool, int, string) {
	requestPath, err := CleanPath(requestPath)
	if err != nil {
		return false, 0, ""
	}
	h.mu.Lock()
	var selected *liveSnapshot
	var object Object
	var exists bool
	matchedRoot := false
	for candidateRootID, candidate := range h.current {
		if rootID != "" && candidateRootID != rootID {
			continue
		}
		if h.retiring[candidateRootID] {
			continue
		}
		if !containsPath(candidate.snapshot.Root, requestPath) {
			continue
		}
		matchedRoot = true
		if candidateObject, ok := candidate.snapshot.byPath[requestPath]; ok && (!exists || len(candidate.snapshot.Root) > len(selected.snapshot.Root)) {
			selected, object, exists = candidate, candidateObject, true
		}
	}
	if !matchedRoot {
		h.mu.Unlock()
		return false, 0, ""
	}
	if !exists {
		h.mu.Unlock()
		if !classifiedMetadata {
			return false, 0, ""
		}
		h.TriggerRefresh()
		w.Header().Set("Retry-After", "5")
		w.Header().Set("X-Cache", "STALE")
		proxyruntime.WriteError(w, http.StatusServiceUnavailable)
		return true, http.StatusServiceUnavailable, "STALE"
	}
	readerKey := selected.snapshot.RootID + "\x00" + selected.snapshot.CandidateID
	h.readers[readerKey]++
	selectedRootID := selected.snapshot.RootID
	cacheControl := strings.ToLower(request.Header.Get("Cache-Control"))
	refreshRequested := requestPath == selected.snapshot.Anchor &&
		(strings.Contains(cacheControl, "no-cache") || strings.Contains(cacheControl, "max-age=0"))
	h.mu.Unlock()
	if refreshRequested {
		h.TriggerRefresh()
	}
	_ = h.touchLastSeen(selectedRootID, time.Now().UTC())
	defer func() {
		h.mu.Lock()
		h.readers[readerKey]--
		if h.readers[readerKey] == 0 {
			delete(h.readers, readerKey)
		}
		h.mu.Unlock()
	}()
	if object.State == ObjectNotFound || object.State == ObjectForbidden {
		status := http.StatusNotFound
		if object.State == ObjectForbidden {
			status = http.StatusForbidden
		}
		http.Error(w, http.StatusText(status), status)
		return true, status, "HIT"
	}
	reader, err := h.config.Store.OpenObject(request.Context(), h.config.Tenant, object.Key)
	if err != nil {
		proxyruntime.WriteError(w, http.StatusServiceUnavailable)
		return true, http.StatusServiceUnavailable, "ERROR"
	}
	defer func() { _ = reader.Close() }()
	copyHeaders(w.Header(), object.Header)
	w.Header().Set("X-Cache", "HIT")
	http.ServeContent(w, request, path.Base(requestPath), selected.snapshot.PublishedAt, reader)
	status := http.StatusOK
	if request.Header.Get("Range") != "" {
		status = http.StatusPartialContent
	}
	return true, status, "HIT"
}

func (h *GenerationManager) touchLastSeen(rootID string, seenAt time.Time) error {
	h.mu.Lock()
	if seenAt.After(h.lastSeen[rootID]) {
		h.lastSeen[rootID] = seenAt
	}
	if persisted := h.lastSeenPersisted[rootID]; !persisted.IsZero() && seenAt.Sub(persisted) < time.Hour {
		h.mu.Unlock()
		return nil
	}
	h.mu.Unlock()

	h.seenPersistMu.Lock()
	defer h.seenPersistMu.Unlock()
	h.mu.RLock()
	latest, exists := h.lastSeen[rootID]
	persisted := h.lastSeenPersisted[rootID]
	h.mu.RUnlock()
	if !exists || !persisted.IsZero() && latest.Sub(persisted) < time.Hour {
		return nil
	}
	prepared, err := prepareJSON(h.config.StateDir, lastSeenName(rootID), lastSeenMarker{RootID: rootID, SeenAt: latest}, maxRepositoryMarkerSize)
	if err != nil {
		return err
	}
	defer prepared.discard()
	h.commitMu.Lock()
	h.mu.RLock()
	_, exists = h.lastSeen[rootID]
	h.mu.RUnlock()
	if !exists {
		h.commitMu.Unlock()
		return nil
	}
	err = prepared.commit()
	if err != nil {
		h.commitMu.Unlock()
		return err
	}
	h.mu.Lock()
	if current := h.lastSeenPersisted[rootID]; !current.After(latest) {
		h.lastSeenPersisted[rootID] = latest
	}
	h.mu.Unlock()
	h.commitMu.Unlock()
	return nil
}

func (h *GenerationManager) Current(rootID string) *Snapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if current := h.current[rootID]; current != nil {
		return current.snapshot
	}
	return nil
}

type RefreshSession struct {
	handler     *GenerationManager
	rootID      string
	root        string
	upstream    string
	generation  string
	candidateID string
	objects     map[string]Object
}

func (s *RefreshSession) Fetch(ctx context.Context, spec ObjectSpec) (*Blob, error) {
	cleaned, err := CleanPath(spec.Path)
	if err != nil || !containsPath(s.root, cleaned) {
		return nil, fmt.Errorf("invalid metadata path %q", spec.Path)
	}
	spec.Path = cleaned
	fetchPath := spec.FetchPath
	if fetchPath == "" {
		fetchPath = cleaned
	}
	fetchPath, err = CleanPath(fetchPath)
	if err != nil || !containsPath(s.root, fetchPath) {
		return nil, fmt.Errorf("invalid metadata fetch path %q", spec.FetchPath)
	}
	maxBytes := spec.MaxBytes
	if maxBytes <= 0 {
		maxBytes = DefaultMaxObject
	}
	if spec.ExpectedSize != nil && (*spec.ExpectedSize < 0 || *spec.ExpectedSize > maxBytes) {
		return nil, fmt.Errorf("metadata %s declared invalid size %d", spec.Path, *spec.ExpectedSize)
	}
	checksums := make([]Checksum, 0, len(spec.Checksums))
	for _, checksum := range spec.Checksums {
		algorithm := strings.ToLower(strings.TrimSpace(checksum.Algorithm))
		digest := strings.ToLower(strings.TrimSpace(checksum.Digest))
		hasher, hashErr := checksumHash(algorithm)
		if hashErr != nil || len(digest) != hasher.Size()*2 {
			return nil, fmt.Errorf("metadata %s has invalid %s checksum", spec.Path, checksum.Algorithm)
		}
		if _, decodeErr := hex.DecodeString(digest); decodeErr != nil {
			return nil, fmt.Errorf("metadata %s has invalid %s checksum", spec.Path, checksum.Algorithm)
		}
		checksums = append(checksums, Checksum{Algorithm: algorithm, Digest: digest})
	}
	var key string
	if len(checksums) > 0 {
		key = candidatePrefix(s.rootID, s.generation, s.candidateID) + "/objects/" + checksums[0].Algorithm + "/" + checksums[0].Digest
	}
	if key != "" {
		if reader, openErr := s.handler.config.Store.OpenObject(ctx, s.handler.config.Tenant, key); openErr == nil {
			hashers := make([]hash.Hash, len(checksums))
			writers := make([]io.Writer, 0, len(checksums)+1)
			internalDigest := sha256.New()
			writers = append(writers, internalDigest)
			for index, checksum := range checksums {
				hashers[index], _ = checksumHash(checksum.Algorithm)
				writers = append(writers, hashers[index])
			}
			size, hashErr := io.Copy(io.MultiWriter(writers...), reader)
			options := reader.Info().Options
			closeErr := reader.Close()
			valid := hashErr == nil && closeErr == nil && (spec.ExpectedSize == nil || size == *spec.ExpectedSize)
			for index, checksum := range checksums {
				valid = valid && hex.EncodeToString(hashers[index].Sum(nil)) == checksum.Digest
			}
			if valid {
				var header http.Header
				_ = json.Unmarshal([]byte(options["header"]), &header)
				object := Object{Path: spec.Path, State: ObjectPresent, Key: key, Size: size, SHA256: hex.EncodeToString(internalDigest.Sum(nil)), Header: header}
				s.objects[spec.Path] = object
				return &Blob{handler: s.handler, object: object}, nil
			}
			_ = s.handler.config.Store.DeleteObject(context.Background(), s.handler.config.Tenant, key)
		}
	}
	response, err := s.handler.config.Fetch(ctx, s.upstream, fetchPath, nil)
	if err != nil {
		return nil, &retryableRefreshError{err: err}
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusForbidden {
		if !spec.Optional {
			return nil, &retryableRefreshError{err: fmt.Errorf("required metadata %s returned %d", spec.Path, response.StatusCode)}
		}
		state := ObjectNotFound
		if response.StatusCode == http.StatusForbidden {
			state = ObjectForbidden
		}
		s.objects[spec.Path] = Object{Path: spec.Path, State: state}
		return nil, nil
	}
	if response.StatusCode != http.StatusOK {
		return nil, &retryableRefreshError{err: fmt.Errorf("metadata %s returned %d", spec.Path, response.StatusCode)}
	}
	hashers := make([]hash.Hash, len(checksums))
	writers := make([]io.Writer, 0, len(checksums))
	for index, checksum := range checksums {
		hashers[index], _ = checksumHash(checksum.Algorithm)
		writers = append(writers, hashers[index])
	}
	source := io.Reader(response.Body)
	if len(writers) > 0 {
		source = io.TeeReader(response.Body, io.MultiWriter(writers...))
	}
	expectedSize := response.ContentLength
	if spec.ExpectedSize != nil {
		expectedSize = *spec.ExpectedSize
	}
	spool, err := s.handler.config.Spooler.SpoolWithExpectedSize(ctx, source, maxBytes, expectedSize)
	if err != nil {
		if errors.Is(err, storeio.ErrObjectTooLarge) {
			return nil, err
		}
		return nil, &retryableRefreshError{err: err}
	}
	defer func() { _ = spool.Close() }()
	if spec.ExpectedSize != nil && spool.Size != *spec.ExpectedSize {
		return nil, fmt.Errorf("metadata %s size mismatch: got %d, want %d", spec.Path, spool.Size, *spec.ExpectedSize)
	}
	for index, checksum := range checksums {
		if actual := hex.EncodeToString(hashers[index].Sum(nil)); actual != checksum.Digest {
			return nil, fmt.Errorf("metadata %s %s mismatch", spec.Path, checksum.Algorithm)
		}
	}
	if key == "" {
		key = candidatePrefix(s.rootID, s.generation, s.candidateID) + "/objects/sha256/" + spool.SHA256
	}
	if err := s.handler.config.Store.MkdirAll(s.handler.config.Tenant+"/"+path.Dir(key), 0o755); err != nil {
		return nil, &retryableRefreshError{err: err}
	}
	if _, err := spool.File.Seek(0, io.SeekStart); err != nil {
		return nil, &retryableRefreshError{err: err}
	}
	encodedHeader, _ := json.Marshal(cloneHeader(response.Header))
	if _, err := s.handler.config.Store.Put(ctx, s.handler.config.Tenant, key, spool.File, map[string]string{"header": string(encodedHeader)}); err != nil {
		return nil, &retryableRefreshError{err: err}
	}
	object := Object{Path: spec.Path, State: ObjectPresent, Key: key, Size: spool.Size, SHA256: spool.SHA256, Header: cloneHeader(response.Header)}
	s.objects[spec.Path] = object
	return &Blob{handler: s.handler, object: object}, nil
}

func (s *RefreshSession) Alias(alias string, blob *Blob) error {
	if blob == nil {
		return errors.New("cannot alias absent metadata")
	}
	cleaned, err := CleanPath(alias)
	if err != nil || !containsPath(s.root, cleaned) {
		return fmt.Errorf("invalid metadata alias %q", alias)
	}
	object := blob.object
	object.Path = cleaned
	s.objects[cleaned] = object
	return nil
}

type Blob struct {
	handler *GenerationManager
	object  Object
}

func (b *Blob) Open(ctx context.Context) (*blobfs.ObjectReader, error) {
	reader, err := b.handler.config.Store.OpenObject(ctx, b.handler.config.Tenant, b.object.Key)
	if err != nil {
		return nil, &retryableRefreshError{err: err}
	}
	return reader, nil
}

func (b *Blob) Size() int64 { return b.object.Size }

func (a Anchor) Open(ctx context.Context) (*blobfs.ObjectReader, error) {
	return a.blob.Open(ctx)
}

func (a Anchor) Size() int64 { return a.blob.Size() }

func copyHeaders(destination, source http.Header) {
	proxyruntime.CopyEndToEndHeaders(destination, source)
	destination.Del("Content-Length")
}

func cloneHeader(source http.Header) http.Header {
	cloned := make(http.Header, len(source))
	copyHeaders(cloned, source)
	return cloned
}

func CleanPath(value string) (string, error) {
	if strings.Contains(value, "\\") || strings.ContainsRune(value, '\x00') || strings.TrimSpace(value) != value || strings.HasPrefix(value, "/") {
		return "", errors.New("invalid repository path")
	}
	cleaned := value
	if cleaned == "" {
		return "", errors.New("invalid repository path")
	}
	for _, segment := range strings.Split(cleaned, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return "", errors.New("invalid repository path")
		}
	}
	return cleaned, nil
}

func cleanRoot(value string) (string, error) {
	if value == "." || value == "" {
		return ".", nil
	}
	return CleanPath(value)
}

func containsPath(root, value string) bool {
	return root == "." || value == root || strings.HasPrefix(value, root+"/")
}

func candidatePrefix(rootID, generation, candidateID string) string {
	sum := sha256.Sum256([]byte(rootID))
	return "generations/" + hex.EncodeToString(sum[:]) + "/" + generation + "/" + candidateID
}

func checksumHash(kind string) (hash.Hash, error) {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "md5":
		return md5.New(), nil
	case "sha", "sha1":
		return sha1.New(), nil
	case "sha256":
		return sha256.New(), nil
	case "sha224":
		return sha256.New224(), nil
	case "sha384":
		return sha512.New384(), nil
	case "sha512":
		return sha512.New(), nil
	default:
		return nil, fmt.Errorf("unsupported metadata checksum %q", kind)
	}
}
