package filerepo

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"path"
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

type Checksum struct {
	Algorithm string
	Digest    string
}

type ObjectSpec struct {
	Path              string
	FetchPath         string
	FallbackFetchPath string
	Aliases           []string
	ExpectedSize      *int64
	MaxBytes          int64
	Checksums         []Checksum
	AllowUnavailable  bool
}

type Object struct {
	Path       string      `json:"path"`
	Key        string      `json:"key"`
	Size       int64       `json:"size"`
	SHA256     string      `json:"sha256"`
	Header     http.Header `json:"header,omitempty"`
	Retainable bool        `json:"retainable,omitempty"`
}

type Snapshot struct {
	ValidUntil  time.Time `json:"valid_until,omitempty"`
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
	Generation string
	Header     http.Header
	blob       *Blob
}

type FetchFunc func(context.Context, string, http.Header) (*http.Response, error)
type BuildFunc func(context.Context, *RefreshSession, Anchor) error

type Config struct {
	RefreshInterval time.Duration
	Instance        string
	Mode            string
	Tenant          string
	Upstream        string
	StateDir        string
	WorkDir         string
	Spooler         *storeio.Spooler
	AnchorMaxBytes  int64
	Store           *blobfs.Store
	Scheduler       *scheduler.Scheduler
	Fetch           FetchFunc
	Build           BuildFunc
	KeepPrevious    int
	GracePeriod     time.Duration
	InactiveAfter   time.Duration
}

type currentMarker struct {
	ValidatedAt    time.Time           `yaml:"validated_at,omitempty"`
	Header         http.Header         `yaml:"header,omitempty"`
	RootID         string              `yaml:"root_id"`
	Root           string              `yaml:"root"`
	Generation     string              `yaml:"generation"`
	CandidateID    string              `yaml:"candidate_id"`
	SnapshotSHA256 string              `yaml:"snapshot_sha256"`
	Upstream       string              `yaml:"upstream"`
	Previous       []snapshotReference `yaml:"previous,omitempty"`
}

type snapshotReference struct {
	Generation     string `yaml:"generation"`
	CandidateID    string `yaml:"candidate_id"`
	SnapshotSHA256 string `yaml:"snapshot_sha256"`
}

type pendingAnchor struct {
	ValidatedAt time.Time   `json:"validated_at"`
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
	polling        bool
	snapshot       *Snapshot
	snapshotSHA256 string
	validatedAt    time.Time
	header         http.Header
	nextCheck      time.Time
	lastAttempt    time.Time
	lastError      error
}

type generationGCPhase uint8

const (
	generationGCIdle generationGCPhase = iota
	generationGCRetire
	generationGCMarkers
	generationGCSnapshots
	generationGCBlobs
)

type retryWindow struct {
	candidateID string
	failures    int
	notBefore   time.Time
}

type GenerationManager struct {
	changed            chan struct{}
	config             Config
	mu                 sync.RWMutex
	current            map[string]*liveSnapshot
	retained           map[string][]*liveSnapshot
	pending            map[string]pendingAnchor
	readers            map[string]int
	lastSeen           map[string]time.Time
	lastSeenPersisted  map[string]time.Time
	retryWindows       map[string]retryWindow
	retiring           map[string]bool
	commitMu           sync.Mutex
	seenPersistMu      sync.Mutex
	discoveryMu        sync.Mutex
	discoveryPending   map[string]bool
	refreshMu          sync.Mutex
	refreshCursor      string
	pollBeforePending  bool
	pollQueue          []string
	pollQueued         map[string]bool
	forceRebuildQueued map[string]bool
	pollCycleActive    bool
	gcMu               sync.Mutex
	gcPhase            generationGCPhase
	gcCursor           string
	gcRetained         map[string]bool
}

type retryableRefreshError struct {
	err error
}

var errUncacheableMetadata = errors.New("metadata response is not cacheable")

type validatedRootKey struct{}

func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return &retryableRefreshError{err: err}
}

func (e *retryableRefreshError) Error() string { return e.err.Error() }
func (e *retryableRefreshError) Unwrap() error { return e.err }

func New(config Config) (*GenerationManager, error) {
	if config.RefreshInterval <= 0 {
		config.RefreshInterval = refreshInterval
	}
	if config.Store == nil || config.Fetch == nil || config.Build == nil || config.StateDir == "" || config.Tenant == "" || config.Upstream == "" {
		return nil, errors.New("filerepo configuration is incomplete")
	}
	upstream, err := url.Parse(config.Upstream)
	if err != nil || upstream.Host == "" || upstream.Scheme != "http" && upstream.Scheme != "https" {
		return nil, errors.New("filerepo upstream must be an absolute HTTP(S) URL")
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
		config:             config,
		current:            make(map[string]*liveSnapshot),
		retained:           make(map[string][]*liveSnapshot),
		pending:            make(map[string]pendingAnchor),
		readers:            make(map[string]int),
		lastSeen:           make(map[string]time.Time),
		lastSeenPersisted:  make(map[string]time.Time),
		retryWindows:       make(map[string]retryWindow),
		retiring:           make(map[string]bool),
		discoveryPending:   make(map[string]bool),
		pollQueued:         make(map[string]bool),
		forceRebuildQueued: make(map[string]bool),
	}
	if err := h.restore(); err != nil {
		return nil, err
	}
	h.changed = make(chan struct{})
	if config.Scheduler != nil {
		config.Scheduler.Register(scheduler.TaskDef{
			Key:      scheduler.NewTaskKey(config.Instance, scheduler.TypeMetadataRefresh, config.Mode),
			Interval: config.RefreshInterval,
			Timeout:  30 * time.Minute,
			Handler: func(ctx context.Context) (*scheduler.TaskOutcome, error) {
				more, err := h.refresh(ctx, 1)
				outcome := &scheduler.TaskOutcome{Result: "success"}
				if more {
					outcome.ContinueAfter = continuationDelay
				} else {
					outcome.ContinueAfter = h.nextRetryDelay(time.Now())
				}
				return outcome, err
			},
		})
		h.TriggerRefresh()
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

func (h *GenerationManager) StageAnchor(ctx context.Context, root, anchorPath string, header http.Header, body io.ReadSeeker) error {
	return h.StageAnchorID(ctx, root, root, anchorPath, header, body)
}

func (h *GenerationManager) StageAnchorID(ctx context.Context, rootID, root, anchorPath string, header http.Header, body io.ReadSeeker) error {
	policy := proxyruntime.ParseCachePolicy(header, time.Now(), 0)
	if policy.NoStore || policy.Private {
		return errUncacheableMetadata
	}
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
	validatedAt, header := storeio.ResponseTimingHeader(ctx, header)
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
	pending := pendingAnchor{
		ValidatedAt: validatedAt,
		RootID:      rootID,
		Root:        root,
		Path:        anchorPath,
		Upstream:    h.config.Upstream,
		Generation:  generation,
		CandidateID: candidateID,
		Header:      cloneHeader(header),
		Key:         key,
	}
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

func (h *GenerationManager) requestCurrentPoll(rootID string, forceRebuild bool) {
	h.mu.Lock()
	_, pending := h.pending[rootID]
	if current := h.current[rootID]; current != nil && !pending && !current.polling {
		if forceRebuild {
			h.forceRebuildQueued[rootID] = true
		}
		if h.pollQueued[rootID] {
			for index, queuedRootID := range h.pollQueue {
				if queuedRootID == rootID {
					copy(h.pollQueue[1:index+1], h.pollQueue[:index])
					h.pollQueue[0] = rootID
					break
				}
			}
		} else {
			h.pollQueued[rootID] = true
			h.pollQueue = append(h.pollQueue, "")
			copy(h.pollQueue[1:], h.pollQueue[:len(h.pollQueue)-1])
			h.pollQueue[0] = rootID
		}
	}
	h.mu.Unlock()
	if !pending {
		h.TriggerRefresh()
	}
}

func (h *GenerationManager) Discover(ctx context.Context, rootID, root, anchorPath string) error {
	response, err := h.config.Fetch(ctx, anchorPath, http.Header{"Accept-Encoding": {"identity"}})
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("metadata discovery anchor %s returned %d", anchorPath, response.StatusCode)
	}
	policy := proxyruntime.ParseCachePolicy(response.Header, time.Now(), 0)
	if policy.NoStore || policy.Private {
		return errUncacheableMetadata
	}
	if encoding := response.Header.Get("Content-Encoding"); encoding != "" && !strings.EqualFold(encoding, "identity") {
		return fmt.Errorf("metadata discovery anchor %s returned content encoding %q", anchorPath, encoding)
	}
	spool, err := h.config.Spooler.SpoolWithExpectedSize(ctx, response.Body, h.config.AnchorMaxBytes, response.ContentLength)
	if err != nil {
		return err
	}
	defer func() { _ = spool.Close() }()
	return h.StageAnchorID(storeio.WithResponseTiming(ctx, response), rootID, root, anchorPath, response.Header, spool.File)
}

func (h *GenerationManager) ScheduleDiscovery(lifecycle *storeio.Lifecycle, rootID, root, anchorPath string) {
	if lifecycle == nil {
		return
	}
	key := rootID + "\x00" + anchorPath
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
		if err := h.Discover(ctx, rootID, root, anchorPath); err != nil {
			slog.Warn("metadata background discovery failed", "mode", h.config.Mode, "path", anchorPath, "err", err)
		}
	})
	if err != nil {
		h.discoveryMu.Lock()
		delete(h.discoveryPending, key)
		h.discoveryMu.Unlock()
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
	http.ServeContent(w, request, path.Base(requestPath), headerModTime(pending.Header), reader)
	return true
}

func (h *GenerationManager) serveCurrent(w http.ResponseWriter, request *http.Request, requestPath string, classifiedMetadata bool, rootID string) (bool, int, string) {
	requestPath, err := CleanPath(requestPath)
	if err != nil {
		return false, 0, ""
	}
	h.mu.Lock()
	var selectedSnapshot *liveSnapshot
	var selectedObject Object
	objectFound := false
	fromCurrent := false
	repositoryMatched := false
	matchedRootID := ""
	matchedRootLength := -1
	for candidateRootID, currentSnapshot := range h.current {
		if rootID != "" && candidateRootID != rootID {
			continue
		}
		if h.retiring[candidateRootID] {
			continue
		}
		if !containsPath(currentSnapshot.snapshot.Root, requestPath) {
			continue
		}
		repositoryMatched = true
		if len(currentSnapshot.snapshot.Root) > matchedRootLength {
			matchedRootID = candidateRootID
			matchedRootLength = len(currentSnapshot.snapshot.Root)
		}
		candidateObject, found := currentSnapshot.snapshot.byPath[requestPath]
		if found && (!objectFound || len(currentSnapshot.snapshot.Root) > len(selectedSnapshot.snapshot.Root)) {
			selectedSnapshot = currentSnapshot
			selectedObject = candidateObject
			objectFound = true
			fromCurrent = true
		}
	}
	if !repositoryMatched {
		h.mu.Unlock()
		return false, 0, ""
	}
	if !objectFound {
		selectedRootLength := -1
		previousObjectAmbiguous := false
		for candidateRootID, previousSnapshots := range h.retained {
			if rootID != "" && candidateRootID != rootID {
				continue
			}
			currentSnapshot := h.current[candidateRootID]
			if currentSnapshot == nil || h.retiring[candidateRootID] || !containsPath(currentSnapshot.snapshot.Root, requestPath) {
				continue
			}
			for _, previousSnapshot := range previousSnapshots {
				candidateObject, found := previousSnapshot.snapshot.byPath[requestPath]
				if !found || !candidateObject.Retainable {
					continue
				}
				rootLength := len(previousSnapshot.snapshot.Root)
				if rootLength < selectedRootLength {
					continue
				}
				if rootLength > selectedRootLength {
					selectedSnapshot = previousSnapshot
					selectedObject = candidateObject
					objectFound = true
					selectedRootLength = rootLength
					previousObjectAmbiguous = false
					continue
				}
				if selectedObject.SHA256 != candidateObject.SHA256 || selectedObject.Size != candidateObject.Size {
					previousObjectAmbiguous = true
				}
			}
		}
		if previousObjectAmbiguous {
			selectedSnapshot = nil
			selectedObject = Object{}
			objectFound = false
		}
	}
	if !objectFound {
		h.mu.Unlock()
		if !classifiedMetadata {
			return false, 0, ""
		}
		h.requestCurrentPoll(matchedRootID, true)
		return false, 0, ""
	}
	readerKey := selectedSnapshot.snapshot.RootID + "\x00" + selectedSnapshot.snapshot.CandidateID
	selectedRootID := selectedSnapshot.snapshot.RootID
	isAnchor := fromCurrent && requestPath == selectedSnapshot.snapshot.Anchor
	if isAnchor && selectedSnapshot.header != nil {
		selectedObject.Header = selectedSnapshot.header
	}
	validated := request.Context().Value(validatedRootKey{}) == selectedRootID
	policy := proxyruntime.ParseCachePolicy(selectedObject.Header, selectedSnapshot.validatedAt, h.config.RefreshInterval)
	expired := !selectedSnapshot.snapshot.ValidUntil.IsZero() && !time.Now().Before(selectedSnapshot.snapshot.ValidUntil)
	strict := isAnchor && !validated && (expired || proxyruntime.RequestForcesRevalidation(request) || policy.NoCache ||
		policy.MustRevalidate && !proxyruntime.ResponseFresh(selectedObject.Header, selectedSnapshot.validatedAt, h.config.RefreshInterval))
	if strict {
		baseline := selectedSnapshot.validatedAt
		h.mu.Unlock()
		if err := h.waitForValidation(request.Context(), selectedRootID, baseline); err != nil {
			status := http.StatusBadGateway
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				status = http.StatusGatewayTimeout
			}
			proxyruntime.WriteError(w, status)
			return true, status, "ERROR"
		}
		request = request.WithContext(context.WithValue(request.Context(), validatedRootKey{}, selectedRootID))
		return h.serveCurrent(w, request, requestPath, classifiedMetadata, rootID)
	}
	if isAnchor && expired {
		h.mu.Unlock()
		proxyruntime.WriteError(w, http.StatusBadGateway)
		return true, http.StatusBadGateway, "ERROR"
	}
	h.readers[readerKey]++
	refreshRequested := isAnchor && !time.Now().Before(selectedSnapshot.nextCheck)
	h.mu.Unlock()
	if refreshRequested {
		h.requestCurrentPoll(selectedRootID, false)
	}
	h.markLastSeen(selectedRootID, time.Now().UTC())
	defer func() {
		h.mu.Lock()
		h.readers[readerKey]--
		if h.readers[readerKey] == 0 {
			delete(h.readers, readerKey)
		}
		h.mu.Unlock()
	}()
	reader, err := h.config.Store.OpenObject(request.Context(), h.config.Tenant, selectedObject.Key)
	if err != nil {
		h.requestCurrentPoll(selectedRootID, true)
		return false, 0, ""
	}
	defer func() { _ = reader.Close() }()
	copyHeaders(w.Header(), selectedObject.Header)
	if isAnchor {
		w.Header().Set("Age", fmt.Sprintf("%d", int64(proxyruntime.ResponseAge(selectedObject.Header, selectedSnapshot.validatedAt, time.Now())/time.Second)))
	}
	w.Header().Set("X-Cache", "HIT")
	http.ServeContent(w, request, path.Base(requestPath), headerModTime(selectedObject.Header), reader)
	status := http.StatusOK
	if request.Header.Get("Range") != "" {
		status = http.StatusPartialContent
	}
	return true, status, "HIT"
}

func (h *GenerationManager) markLastSeen(rootID string, seenAt time.Time) {
	h.mu.Lock()
	if seenAt.After(h.lastSeen[rootID]) {
		h.lastSeen[rootID] = seenAt
	}
	h.mu.Unlock()
}

func (h *GenerationManager) flushLastSeen(ctx context.Context) error {
	h.seenPersistMu.Lock()
	defer h.seenPersistMu.Unlock()
	h.mu.RLock()
	dirty := make(map[string]time.Time)
	for rootID, latest := range h.lastSeen {
		persisted := h.lastSeenPersisted[rootID]
		if persisted.IsZero() || latest.Sub(persisted) >= time.Hour {
			dirty[rootID] = latest
		}
	}
	h.mu.RUnlock()
	for rootID, seenAt := range dirty {
		if err := ctx.Err(); err != nil {
			return err
		}
		prepared, err := prepareJSON(h.config.StateDir, lastSeenName(rootID), lastSeenMarker{RootID: rootID, SeenAt: seenAt}, maxRepositoryMarkerSize)
		if err != nil {
			return err
		}
		h.commitMu.Lock()
		h.mu.RLock()
		_, exists := h.lastSeen[rootID]
		h.mu.RUnlock()
		if exists {
			err = prepared.commit()
		}
		if err == nil && exists {
			h.mu.Lock()
			if current := h.lastSeenPersisted[rootID]; !current.After(seenAt) {
				h.lastSeenPersisted[rootID] = seenAt
			}
			h.mu.Unlock()
		}
		h.commitMu.Unlock()
		prepared.discard()
		if err != nil {
			return err
		}
	}
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

func copyHeaders(destination, source http.Header) {
	proxyruntime.CopyEndToEndHeaders(destination, source)
	destination.Del("Content-Length")
}

func headerModTime(header http.Header) time.Time {
	modified, err := http.ParseTime(header.Get("Last-Modified"))
	if err != nil {
		return time.Time{}
	}
	return modified
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
