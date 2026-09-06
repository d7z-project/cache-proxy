package filerepo

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func FuzzPollTimingAndCancellation(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3})
	f.Add([]byte{255, 0, 255, 1, 7, 9})
	f.Add([]byte("cancel-and-recovery"))
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 256 {
			t.Skip()
		}
		var value uint64
		for _, byteValue := range input {
			value = value<<5 ^ uint64(byteValue)
		}
		interval := time.Duration(1 + value%uint64(7*24*time.Hour))
		rootID := "fuzz-root"
		validated := time.Unix(int64(value%uint64(365*24*60*60))-180*24*60*60, int64(value%1_000_000_000)).UTC()
		published := validated.Add(time.Duration(value % uint64(2*time.Hour)))
		h := &GenerationManager{
			config: Config{Instance: "fuzz", RefreshInterval: interval},
			current: map[string]*liveSnapshot{
				rootID: {snapshot: &Snapshot{RootID: rootID, CandidateID: "candidate", Generation: "generation"}, validatedAt: validated, nextPoll: time.Now().Add(time.Hour)},
			},
			retryWindows: make(map[string]retryWindow),
			pollRequests: make(map[string]pollRequest),
			changed:      make(chan struct{}),
		}

		next := h.nextPollAt(rootID, validated, published)
		base := validated
		if published.After(base) {
			base = published
		}
		bound := min(interval/20, time.Minute)
		require.GreaterOrEqual(t, next.Sub(base), interval)
		require.LessOrEqual(t, next.Sub(base), interval+bound)
		require.Equal(t, next, h.nextPollAt(rootID, validated, published))

		var triggers sync.WaitGroup
		force := value&1 != 0
		for index := range 4 {
			triggers.Go(func() { h.requestCurrentPoll(rootID, force && index == 0, time.Time{}) })
		}
		triggers.Wait()
		h.mu.RLock()
		require.Len(t, h.pollQueue, 1)
		poll := h.pollRequests[rootID]
		h.mu.RUnlock()
		if force {
			require.True(t, poll.forceRebuild)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cancel()
		h.config.Fetch = func(ctx context.Context, _ string, _ http.Header) (*http.Response, error) {
			return nil, ctx.Err()
		}
		beforeCandidate := h.current[rootID].snapshot.CandidateID
		_, _, err := h.runRefresh(ctx, 1)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, beforeCandidate, h.current[rootID].snapshot.CandidateID)
		require.False(t, errors.Is(err, context.DeadlineExceeded))
	})
}
