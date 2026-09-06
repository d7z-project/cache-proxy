package filerepo

import (
	"context"
	"errors"
	"os"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
)

var errValidationWait = errors.New("metadata validation wait ended")

// Waiting requests do not own scheduler work or candidate lifetime.
func (h *GenerationManager) waitForValidation(ctx context.Context, rootID string, baseline time.Time, validation scheduler.ValidationDetails) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	h.requestCurrentPoll(rootID, false, baseline, validation)
	for {
		h.mu.RLock()
		current := h.current[rootID]
		candidate, pending := h.pending[rootID]
		changed := h.changed
		var err error
		complete := current == nil || !pending && current.validatedAt.After(baseline)
		if current != nil {
			candidateID := current.snapshot.CandidateID
			if pending {
				candidateID = candidate.CandidateID
			}
			retry := h.retryWindows[rootID]
			if retry.candidateID == candidateID && !current.polling && time.Now().Before(retry.notBefore) {
				err = retry.err
			}
		}
		h.mu.RUnlock()
		if complete {
			return nil
		}
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return errors.Join(errValidationWait, err)
		}
		if h.config.Scheduler == nil {
			if _, _, err := h.runRefresh(ctx, 1); err != nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
			// Read completion once more before reporting the wait deadline.
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
	h.removePollLocked(rootID)
	h.mu.Unlock()
	return nil
}
