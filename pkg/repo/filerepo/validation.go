package filerepo

import (
	"context"
	"errors"
	"os"
	"time"
)

// Waiting requests do not own scheduler work or candidate lifetime.
func (h *GenerationManager) waitForValidation(ctx context.Context, rootID string, baseline time.Time) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	started := time.Now()
	h.requestCurrentPoll(rootID, false)
	for {
		h.mu.RLock()
		current := h.current[rootID]
		_, pending := h.pending[rootID]
		changed := h.changed
		var err error
		complete := current == nil || !pending && current.validatedAt.After(baseline)
		if current != nil && current.lastAttempt.After(started) {
			err = current.lastError
		}
		h.mu.RUnlock()
		if complete || err != nil {
			return err
		}
		if h.config.Scheduler == nil {
			if _, err := h.refresh(ctx, 1); err != nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// Retiring routing precedes bypass. An already building candidate cannot commit
// after its pending identity has been removed under commitMu.
func (h *GenerationManager) retireCachedRoot(rootID string) error {
	h.commitMu.Lock()
	defer h.commitMu.Unlock()
	for _, name := range []string{currentName(rootID), pendingName(rootID)} {
		if err := os.Remove(statePath(h.config.StateDir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	h.mu.Lock()
	delete(h.current, rootID)
	delete(h.pending, rootID)
	delete(h.retained, rootID)
	delete(h.retryWindows, rootID)
	delete(h.pollQueued, rootID)
	delete(h.forceRebuildQueued, rootID)
	queue := h.pollQueue[:0]
	for _, queued := range h.pollQueue {
		if queued != rootID {
			queue = append(queue, queued)
		}
	}
	clear(h.pollQueue[len(queue):])
	h.pollQueue = queue
	h.mu.Unlock()
	return nil
}
