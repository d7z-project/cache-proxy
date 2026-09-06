package filerepo

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

// Refresh starts or continues an explicit polling cycle and processes up to limit items.
func (h *GenerationManager) Refresh(ctx context.Context, limit int) (bool, error) {
	h.mu.Lock()
	if len(h.pollQueue) == 0 && !h.pollCycleActive {
		roots := make([]string, 0, len(h.current))
		for rootID := range h.current {
			if _, pending := h.pending[rootID]; !pending {
				roots = append(roots, rootID)
			}
		}
		sort.Strings(roots)
		for _, rootID := range roots {
			h.enqueuePollLocked(rootID, false)
		}
		h.pollCycleActive = len(h.pollQueue) > 0
	}
	h.mu.Unlock()
	more, _, err := h.runRefresh(ctx, limit)
	return more, err
}

// runRefresh only processes requested or due work; it does not start a polling cycle.
func (h *GenerationManager) runRefresh(ctx context.Context, limit int) (bool, *scheduler.TaskOutcome, error) {
	h.refreshMu.Lock()
	defer h.refreshMu.Unlock()
	outcome := &scheduler.TaskOutcome{NoWork: true}
	defer func() {
		h.mu.Lock()
		close(h.changed)
		h.changed = make(chan struct{})
		h.mu.Unlock()
	}()
	if err := h.flushLastSeen(ctx); err != nil {
		return false, outcome, err
	}
	if limit <= 0 {
		h.mu.RLock()
		limit = len(h.pending) + len(h.current) + len(h.pollQueue) + 1
		h.mu.RUnlock()
	}
	processed := 0
	processedPending := false
	for processed < limit {
		var rootID string
		var poll pollRequest
		var pollReady bool
		if h.pollBeforePending {
			h.pollBeforePending = false
			rootID, poll, pollReady = h.nextCurrentPoll(time.Now())
		}
		if !pollReady {
			if pending, retry, ok := h.nextPending(time.Now()); ok {
				h.pollBeforePending = true
				processed++
				processedPending = true
				if err := h.refreshPending(ctx, pending, retry, outcome); err != nil {
					h.recordPendingFailure(pending.RootID, pending.CandidateID, err)
					return h.hasRunnableRefresh(time.Now()), outcome, err
				}
				continue
			}
			if processedPending {
				break
			}
			rootID, poll, pollReady = h.nextCurrentPoll(time.Now())
		}
		if !pollReady {
			break
		}
		processed++
		*outcome = scheduler.TaskOutcome{Validation: poll.validation, Target: rootID, Reason: poll.reason, Phase: "check", QueueDuration: max(0, time.Since(poll.readyAt))}
		if err := h.pollCurrent(ctx, rootID, poll.forceRebuild, outcome); err != nil {
			return h.hasRunnableRefresh(time.Now()), outcome, err
		}
	}
	h.mu.Lock()
	if len(h.pollQueue) == 0 {
		h.pollCycleActive = false
	}
	h.mu.Unlock()
	return h.hasRunnableRefresh(time.Now()), outcome, nil
}

func (h *GenerationManager) refreshPending(ctx context.Context, pending pendingAnchor, retry retryWindow, outcome *scheduler.TaskOutcome) error {
	readyAt := pending.queuedAt
	if readyAt.IsZero() {
		readyAt = time.Now()
	}
	if retry.notBefore.After(readyAt) {
		readyAt = retry.notBefore
	}
	*outcome = scheduler.TaskOutcome{Validation: pending.validation, Target: pending.RootID, Reason: pending.reason, Phase: "build", Result: "published", QueueDuration: max(0, time.Since(readyAt))}
	defer func() {
		h.mu.Lock()
		if live, ok := h.pending[pending.RootID]; ok && live.CandidateID == pending.CandidateID {
			outcome.Validation.Merge(live.validation)
		}
		if live := h.current[pending.RootID]; live != nil && live.snapshot.CandidateID == pending.CandidateID {
			outcome.Validation.Merge(live.validation)
			live.validation = scheduler.ValidationDetails{}
		}
		h.mu.Unlock()
	}()
	if outcome.Reason == "" {
		outcome.Reason = "initial"
	}
	if retry.candidateID == pending.CandidateID && retry.failures > 0 {
		outcome.Reason, outcome.Phase = "retry", "check"
		updated, replaced, err := h.revalidatePending(ctx, pending)
		if err != nil {
			return err
		}
		if replaced {
			outcome.Result = "staged"
			h.mu.RLock()
			if _, exists := h.pending[pending.RootID]; !exists {
				outcome.Result = "bypassed"
			}
			h.mu.RUnlock()
			return nil
		}
		pending = updated
		outcome.Phase = "build"
	}
	if err := h.refreshRoot(ctx, pending); err != nil {
		return err
	}
	h.mu.Lock()
	delete(h.retryWindows, pending.RootID)
	if h.current[pending.RootID] == nil {
		outcome.Result = "bypassed"
	}
	h.mu.Unlock()
	return nil
}

func (h *GenerationManager) nextPending(now time.Time) (pendingAnchor, retryWindow, bool) {
	h.mu.RLock()
	roots := make([]string, 0, len(h.pending))
	for rootID, pending := range h.pending {
		retry := h.retryWindows[rootID]
		if retry.candidateID != pending.CandidateID || !now.Before(retry.notBefore) {
			roots = append(roots, rootID)
		}
	}
	sort.Strings(roots)
	if h.refreshCursor != "" && len(roots) > 1 {
		start := sort.Search(len(roots), func(i int) bool { return roots[i] > h.refreshCursor })
		if start == len(roots) {
			start = 0
		}
		roots = append(roots[start:], roots[:start]...)
	}
	if len(roots) == 0 {
		h.mu.RUnlock()
		return pendingAnchor{}, retryWindow{}, false
	}
	rootID := roots[0]
	pending := h.pending[rootID]
	retry := h.retryWindows[rootID]
	h.mu.RUnlock()
	h.refreshCursor = rootID
	return pending, retry, true
}

func (h *GenerationManager) nextCurrentPoll(now time.Time) (string, pollRequest, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	roots := make([]string, 0, len(h.current))
	for rootID, current := range h.current {
		if _, pending := h.pending[rootID]; !pending && !now.Before(current.nextPoll) {
			roots = append(roots, rootID)
		}
	}
	sort.Strings(roots)
	for _, rootID := range roots {
		h.enqueuePollLocked(rootID, false)
	}
	h.pollCycleActive = len(h.pollQueue) != 0
	for index, rootID := range h.pollQueue {
		current := h.current[rootID]
		if current == nil {
			delete(h.pollRequests, rootID)
			continue
		}
		poll, queued := h.pollRequests[rootID]
		if !queued {
			continue
		}
		if _, pending := h.pending[rootID]; pending {
			continue
		}
		retry := h.retryWindows[rootID]
		if retry.candidateID == current.snapshot.CandidateID && now.Before(retry.notBefore) {
			continue
		}
		copy(h.pollQueue[index:], h.pollQueue[index+1:])
		h.pollQueue[len(h.pollQueue)-1] = ""
		h.pollQueue = h.pollQueue[:len(h.pollQueue)-1]
		delete(h.pollRequests, rootID)
		if retry.candidateID == current.snapshot.CandidateID && retry.failures > 0 {
			poll.reason = "retry"
			if retry.notBefore.After(poll.readyAt) {
				poll.readyAt = retry.notBefore
			}
		}
		current.polling = true
		current.pollingRebuild = poll.forceRebuild
		current.validation = poll.validation
		return rootID, poll, true
	}
	compacted := h.pollQueue[:0]
	for _, rootID := range h.pollQueue {
		if _, queued := h.pollRequests[rootID]; queued && h.current[rootID] != nil {
			compacted = append(compacted, rootID)
		}
	}
	clear(h.pollQueue[len(compacted):])
	h.pollQueue = compacted
	if len(h.pollQueue) == 0 {
		h.pollCycleActive = false
	}
	return "", pollRequest{}, false
}

func (h *GenerationManager) recordPendingFailure(rootID, candidateID string, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	pending, exists := h.pending[rootID]
	if !exists || pending.CandidateID != candidateID {
		return
	}
	h.retryWindows[rootID] = h.retryWindows[rootID].afterFailure(candidateID, err)
}

func (retry retryWindow) afterFailure(candidateID string, err error) retryWindow {
	if retry.candidateID != candidateID {
		retry = retryWindow{candidateID: candidateID}
	}
	retry.failures++
	retry.err = err
	retry.notBefore = time.Now().Add(refreshRetryDelay(candidateID, retry.failures))
	return retry
}

func refreshRetryDelay(candidateID string, failures int) time.Duration {
	if failures < 1 {
		failures = 1
	}
	delay := retryDelay << min(failures-1, 4)
	if delay > 15*time.Minute {
		delay = 15 * time.Minute
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", candidateID, failures)))
	jitterPercent := int(digest[0])%21 - 10
	delay = delay * time.Duration(100+jitterPercent) / 100
	return min(delay, 15*time.Minute)
}

func (h *GenerationManager) hasRunnableRefresh(now time.Time) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for rootID, pending := range h.pending {
		retry := h.retryWindows[rootID]
		if retry.candidateID != pending.CandidateID || !now.Before(retry.notBefore) {
			return true
		}
	}
	for _, rootID := range h.pollQueue {
		if _, queued := h.pollRequests[rootID]; !queued {
			continue
		}
		if _, pending := h.pending[rootID]; pending {
			continue
		}
		if current := h.current[rootID]; current != nil {
			retry := h.retryWindows[rootID]
			if retry.candidateID != current.snapshot.CandidateID || !now.Before(retry.notBefore) {
				return true
			}
		}
	}
	return false
}

func (h *GenerationManager) nextRefreshDelay(now time.Time) time.Duration {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var next time.Duration
	for rootID, current := range h.current {
		if _, pending := h.pending[rootID]; pending {
			continue
		}
		due := current.nextPoll
		if retry := h.retryWindows[rootID]; retry.notBefore.After(due) {
			due = retry.notBefore
		}
		wait := max(time.Second, due.Sub(now))
		if next == 0 || wait < next {
			next = wait
		}
	}
	for rootID, retry := range h.retryWindows {
		pending := h.pending[rootID]
		current := h.current[rootID]
		active := pending.CandidateID == retry.candidateID || current != nil && current.snapshot.CandidateID == retry.candidateID
		if wait := retry.notBefore.Sub(now); active && wait > 0 && (next == 0 || wait < next) {
			next = wait
		}
	}
	return next
}

func (h *GenerationManager) enqueuePollLocked(rootID string, forceRebuild bool) bool {
	if rootID == "" {
		return false
	}
	poll, queued := h.pollRequests[rootID]
	if queued {
		if forceRebuild && !poll.forceRebuild {
			poll.forceRebuild, poll.reason = true, "recovery"
			h.pollRequests[rootID] = poll
			return true
		}
		return false
	}
	poll = pollRequest{reason: "periodic", readyAt: time.Now(), forceRebuild: forceRebuild}
	if current := h.current[rootID]; current != nil && current.nextPoll.Before(poll.readyAt) {
		poll.readyAt = current.nextPoll
	}
	if forceRebuild {
		poll.reason, poll.readyAt = "recovery", time.Now()
	}
	h.pollRequests[rootID] = poll
	h.pollQueue = append(h.pollQueue, rootID)
	return true
}

func (h *GenerationManager) removePollLocked(rootID string) {
	delete(h.pollRequests, rootID)
	queued := h.pollQueue[:0]
	for _, root := range h.pollQueue {
		if root != rootID {
			queued = append(queued, root)
		}
	}
	clear(h.pollQueue[len(queued):])
	h.pollQueue = queued
}

func (h *GenerationManager) revalidatePending(ctx context.Context, pending pendingAnchor) (pendingAnchor, bool, error) {
	validators := make(http.Header)
	if value := pending.Header.Get("ETag"); value != "" {
		validators.Set("If-None-Match", value)
	}
	if value := pending.Header.Get("Last-Modified"); value != "" {
		validators.Set("If-Modified-Since", value)
	}
	validators.Set("Accept-Encoding", "identity")
	response, err := h.config.Fetch(ctx, pending.Path, validators)
	if err != nil {
		return pending, false, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode == http.StatusNotModified {
		pending.ValidatedAt, response.Header = storeio.ResponseTimingHeader(storeio.WithResponseTiming(ctx, response), response.Header)
		pending.Header = proxyruntime.MergeRevalidationHeader(pending.Header, response.Header)
		policy := proxyruntime.ParseCachePolicy(pending.Header, pending.ValidatedAt, 0)
		if policy.NoStore || policy.Private {
			return pending, true, h.retireCachedRoot(pending.RootID)
		}
		return pending, false, nil
	}
	if response.StatusCode != http.StatusOK {
		return pending, false, fmt.Errorf("metadata anchor %s returned %d", pending.Path, response.StatusCode)
	}
	policy := proxyruntime.ParseCachePolicy(response.Header, time.Now(), 0)
	if policy.NoStore || policy.Private {
		return pending, true, h.retireCachedRoot(pending.RootID)
	}
	if encoding := response.Header.Get("Content-Encoding"); encoding != "" && !strings.EqualFold(encoding, "identity") {
		return pending, false, fmt.Errorf("metadata anchor %s returned content encoding %q", pending.Path, encoding)
	}
	spool, err := h.config.Spooler.SpoolWithExpectedSize(ctx, response.Body, h.config.AnchorMaxBytes, response.ContentLength)
	if err != nil {
		return pending, false, err
	}
	defer func() { _ = spool.Close() }()
	if spool.SHA256 == pending.Generation {
		pending.ValidatedAt, pending.Header = storeio.ResponseTimingHeader(storeio.WithResponseTiming(ctx, response), response.Header)
		h.mu.Lock()
		if current, exists := h.pending[pending.RootID]; exists && current.CandidateID == pending.CandidateID {
			current.Header = pending.Header
			current.ValidatedAt = pending.ValidatedAt
			h.pending[pending.RootID] = current
		}
		h.mu.Unlock()
		return pending, false, nil
	}
	if err := h.stageAnchor(storeio.WithResponseTiming(ctx, response), pending.RootID, pending.Root, pending.Path, response.Header, spool.File, "retry", false); err != nil {
		return pending, false, err
	}
	return pending, true, nil
}

func (h *GenerationManager) pollCurrent(ctx context.Context, rootID string, forceRebuild bool, outcome *scheduler.TaskOutcome) (pollErr error) {
	h.mu.RLock()
	var snapshot *Snapshot
	if current := h.current[rootID]; current != nil {
		snapshot = current.snapshot
	}
	h.mu.RUnlock()
	if snapshot == nil {
		outcome.NoWork = true
		return nil
	}
	outcome.Result = "unchanged"
	defer func() { h.finishCurrentPoll(snapshot, pollErr, forceRebuild, outcome) }()

	validators := make(http.Header)
	validators.Set("Cache-Control", "no-cache")
	if anchor, ok := snapshot.byPath[snapshot.Anchor]; ok {
		h.mu.RLock()
		if live := h.current[rootID]; live != nil && live.snapshot.CandidateID == snapshot.CandidateID && live.header != nil {
			anchor.Header = live.header
		}
		h.mu.RUnlock()
		if value := anchor.Header.Get("ETag"); value != "" {
			validators.Set("If-None-Match", value)
		}
		if value := anchor.Header.Get("Last-Modified"); value != "" {
			validators.Set("If-Modified-Since", value)
		}
	}
	response, err := h.config.Fetch(ctx, snapshot.Anchor, validators)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNotModified {
		_ = response.Body.Close()
		return fmt.Errorf("metadata anchor %s returned %d", snapshot.Anchor, response.StatusCode)
	}
	policy := proxyruntime.ParseCachePolicy(response.Header, time.Now(), 0)
	if policy.NoStore || policy.Private {
		_ = response.Body.Close()
		outcome.Result = "bypassed"
		return h.retireCachedRoot(rootID)
	}
	if response.StatusCode == http.StatusNotModified {
		_ = response.Body.Close()
		if forceRebuild {
			outcome.Result = "staged"
			anchor := snapshot.byPath[snapshot.Anchor]
			reader, err := h.config.Store.OpenObject(ctx, h.config.Tenant, anchor.Key)
			if err == nil {
				err = h.stageAnchor(storeio.WithResponseTiming(ctx, response), snapshot.RootID, snapshot.Root, snapshot.Anchor, proxyruntime.MergeRevalidationHeader(anchor.Header, response.Header), reader, outcome.Reason, false)
				_ = reader.Close()
			}
			return err
		}
		return h.updateCurrentFreshness(storeio.WithResponseTiming(ctx, response), snapshot.RootID, snapshot.CandidateID, response.Header)
	}
	spool, err := h.config.Spooler.SpoolWithExpectedSize(ctx, response.Body, h.config.AnchorMaxBytes, response.ContentLength)
	_ = response.Body.Close()
	if err != nil {
		return err
	}
	if spool.SHA256 == snapshot.Generation {
		if forceRebuild {
			outcome.Result = "staged"
			err = h.stageAnchor(storeio.WithResponseTiming(ctx, response), snapshot.RootID, snapshot.Root, snapshot.Anchor, response.Header, spool.File, outcome.Reason, false)
			_ = spool.Close()
			return err
		}
		_ = spool.Close()
		return h.updateCurrentFreshness(storeio.WithResponseTiming(ctx, response), snapshot.RootID, snapshot.CandidateID, response.Header)
	}
	outcome.Result = "staged"
	err = h.stageAnchor(storeio.WithResponseTiming(ctx, response), snapshot.RootID, snapshot.Root, snapshot.Anchor, response.Header, spool.File, outcome.Reason, false)
	_ = spool.Close()
	return err
}

func (h *GenerationManager) finishCurrentPoll(snapshot *Snapshot, pollErr error, forceRebuild bool, outcome *scheduler.TaskOutcome) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if current := h.current[snapshot.RootID]; current != nil && current.snapshot.CandidateID == snapshot.CandidateID {
		outcome.Validation.Merge(current.validation)
		current.polling = false
		current.pollingRebuild = false
		current.validation = scheduler.ValidationDetails{}
	}
	if pollErr == nil {
		delete(h.retryWindows, snapshot.RootID)
		return
	}
	if current := h.current[snapshot.RootID]; current != nil && current.snapshot.CandidateID == snapshot.CandidateID {
		h.retryWindows[snapshot.RootID] = h.retryWindows[snapshot.RootID].afterFailure(snapshot.CandidateID, pollErr)
		h.enqueuePollLocked(snapshot.RootID, forceRebuild)
		queued := h.pollRequests[snapshot.RootID]
		queued.validation.Merge(outcome.Validation)
		h.pollRequests[snapshot.RootID] = queued
		h.pollCycleActive = true
	}
}

func (h *GenerationManager) updateCurrentFreshness(ctx context.Context, rootID, candidateID string, header http.Header) error {
	h.commitMu.Lock()
	defer h.commitMu.Unlock()
	h.mu.RLock()
	live := h.current[rootID]
	var updated liveSnapshot
	if live != nil {
		updated = *live
	}
	h.mu.RUnlock()
	if live == nil || live.snapshot.CandidateID != candidateID {
		return nil
	}
	validatedAt, update := storeio.ResponseTimingHeader(ctx, header)
	stored := updated.header
	if stored == nil {
		stored = updated.snapshot.byPath[updated.snapshot.Anchor].Header
	}
	merged := proxyruntime.MergeRevalidationHeader(stored, update)
	var marker currentMarker
	if err := readYAML(h.config.StateDir, currentName(rootID), &marker); err != nil {
		return err
	}
	if marker.CandidateID != candidateID {
		return errors.New("metadata current marker changed during validation")
	}
	marker.ValidatedAt, marker.Header = validatedAt, merged
	prepared, err := prepareYAML(h.config.StateDir, currentName(rootID), marker)
	if err != nil {
		return err
	}
	defer prepared.discard()
	if err := prepared.commit(); err != nil {
		return err
	}
	updated.validatedAt, updated.header = validatedAt, merged
	updated.nextPoll = h.nextPollAt(rootID, validatedAt, updated.snapshot.PublishedAt)
	h.mu.Lock()
	if live := h.current[rootID]; live != nil && live.snapshot.CandidateID == candidateID {
		updated.validation.Merge(live.validation)
	}
	h.current[rootID] = &updated
	h.mu.Unlock()
	return nil
}

func (h *GenerationManager) pollOffset(rootID string) time.Duration {
	digest := sha256.Sum256([]byte(h.config.Instance + "\x00" + rootID))
	return min(h.config.RefreshInterval/20, time.Minute) / 255 * time.Duration(digest[0])
}

func (h *GenerationManager) nextPollAt(rootID string, validated, published time.Time) time.Time {
	if published.After(validated) {
		validated = published
	}
	return validated.Add(h.config.RefreshInterval).Add(h.pollOffset(rootID))
}

func (h *GenerationManager) refreshRoot(ctx context.Context, pending pendingAnchor) error {
	rootID := pending.RootID
	anchorReader, err := h.config.Store.OpenObject(ctx, h.config.Tenant, pending.Key)
	if err != nil {
		return fmt.Errorf("open staged anchor: %w", err)
	}
	anchorBlob := &Blob{
		handler: h,
		object: Object{
			Path:   pending.Path,
			Key:    pending.Key,
			Size:   anchorReader.Info().Size,
			SHA256: pending.Generation,
			Header: pending.Header,
		},
	}
	_ = anchorReader.Close()
	session := &RefreshSession{
		handler:     h,
		rootID:      rootID,
		root:        pending.Root,
		generation:  pending.Generation,
		candidateID: pending.CandidateID,
		objects:     make(map[string]Object),
	}
	session.objects[pending.Path] = anchorBlob.object
	anchor := Anchor{RootID: rootID, Root: pending.Root, Path: pending.Path, Generation: pending.Generation, Header: pending.Header, blob: anchorBlob}
	if err := h.config.Build(ctx, session, anchor); err != nil {
		if errors.Is(err, errUncacheableMetadata) {
			return h.retireCachedRoot(rootID)
		}
		var retryableError *retryableRefreshError
		if !errors.As(err, &retryableError) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			h.discardCandidate(rootID, pending, err)
		}
		return err
	}
	objects := make([]Object, 0, len(session.objects))
	for _, object := range session.objects {
		objects = append(objects, object)
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Path < objects[j].Path })
	snapshot := &Snapshot{
		ValidUntil:  session.ValidUntil,
		RootID:      rootID,
		Root:        pending.Root,
		Anchor:      pending.Path,
		Generation:  pending.Generation,
		CandidateID: pending.CandidateID,
		Upstream:    h.config.Upstream,
		PublishedAt: time.Now().UTC(),
		Objects:     objects,
	}
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
	h.commitMu.Lock()
	h.mu.RLock()
	latest, stillPending := h.pending[rootID]
	sameCandidate := stillPending && latest.CandidateID == pending.CandidateID
	previousSnapshots := make([]*liveSnapshot, 0, h.config.KeepPrevious)
	seenCandidateIDs := map[string]struct{}{pending.CandidateID: {}}
	if current := h.current[rootID]; current != nil && current.snapshot.CandidateID != pending.CandidateID {
		previousSnapshots = append(previousSnapshots, current)
		seenCandidateIDs[current.snapshot.CandidateID] = struct{}{}
	}
	for _, retainedSnapshot := range h.retained[rootID] {
		if len(previousSnapshots) >= h.config.KeepPrevious {
			break
		}
		candidateID := retainedSnapshot.snapshot.CandidateID
		if _, duplicate := seenCandidateIDs[candidateID]; !duplicate {
			previousSnapshots = append(previousSnapshots, retainedSnapshot)
			seenCandidateIDs[candidateID] = struct{}{}
		}
	}
	h.mu.RUnlock()
	if !sameCandidate {
		h.commitMu.Unlock()
		h.discardCandidate(rootID, pending, nil)
		return errors.New("metadata anchor changed during refresh")
	}
	marker := currentMarker{
		ValidatedAt:    pending.ValidatedAt,
		Header:         pending.Header,
		RootID:         rootID,
		Root:           pending.Root,
		Generation:     pending.Generation,
		CandidateID:    pending.CandidateID,
		SnapshotSHA256: digest,
		Upstream:       h.config.Upstream,
		Previous:       make([]snapshotReference, 0, len(previousSnapshots)),
	}
	for _, previousSnapshot := range previousSnapshots {
		marker.Previous = append(marker.Previous, snapshotReference{
			Generation:     previousSnapshot.snapshot.Generation,
			CandidateID:    previousSnapshot.snapshot.CandidateID,
			SnapshotSHA256: previousSnapshot.snapshotSHA256,
		})
	}
	preparedMarker, err := prepareYAML(h.config.StateDir, currentName(rootID), marker)
	if err != nil {
		h.commitMu.Unlock()
		return err
	}
	defer preparedMarker.discard()
	if err := preparedMarker.commit(); err != nil {
		h.commitMu.Unlock()
		return err
	}
	h.mu.Lock()
	h.current[rootID] = &liveSnapshot{validation: h.pending[rootID].validation, snapshot: snapshot, snapshotSHA256: digest, validatedAt: pending.ValidatedAt, header: pending.Header, nextPoll: h.nextPollAt(rootID, pending.ValidatedAt, snapshot.PublishedAt)}
	h.removePollLocked(rootID)
	if len(previousSnapshots) == 0 {
		delete(h.retained, rootID)
	} else {
		h.retained[rootID] = previousSnapshots
	}
	delete(h.pending, rootID)
	h.mu.Unlock()
	_ = os.Remove(statePath(h.config.StateDir, pendingName(rootID)))
	h.commitMu.Unlock()
	return nil
}

func (h *GenerationManager) discardCandidate(rootID string, pending pendingAnchor, buildErr error) {
	h.commitMu.Lock()
	defer h.commitMu.Unlock()
	h.mu.Lock()
	latest, exists := h.pending[rootID]
	sameCandidate := exists && latest.CandidateID == pending.CandidateID
	if sameCandidate {
		// The candidate is discarded, but validation of the still-current snapshot
		// failed. Preserve that result until its next check or a replacement arrives.
		if current := h.current[rootID]; current != nil && buildErr != nil {
			candidateID := current.snapshot.CandidateID
			h.retryWindows[rootID] = (retryWindow{}).afterFailure(candidateID, buildErr)
			h.enqueuePollLocked(rootID, false)
			queued := h.pollRequests[rootID]
			queued.validation.Merge(latest.validation)
			h.pollRequests[rootID] = queued
		}
		delete(h.pending, rootID)
	}
	h.mu.Unlock()
	if sameCandidate {
		_ = os.Remove(statePath(h.config.StateDir, pendingName(rootID)))
	}
}
