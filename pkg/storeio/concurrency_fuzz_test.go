package storeio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	goruntime "runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

func FuzzFlightGroupParallel(f *testing.F) {
	f.Add(uint8(1), uint8(1), uint64(0), false)
	f.Add(uint8(16), uint8(4), uint64(0xaaaa), true)
	f.Fuzz(func(t *testing.T, workerInput, keyInput uint8, cancelMask uint64, fail bool) {
		workerCount := int(workerInput%24) + 1
		keyCount := min(int(keyInput%4)+1, workerCount)
		group := &FlightGroup{}
		starts := make(chan struct{})
		results := make(chan struct {
			index  int
			key    string
			flight *Flight
			leader bool
		}, workerCount)

		var beginWait sync.WaitGroup
		for i := range workerCount {
			beginWait.Add(1)
			go func(index int) {
				defer beginWait.Done()
				<-starts
				key := fmt.Sprintf("object-%d", index%keyCount)
				flight, leader := group.Begin(key)
				results <- struct {
					index  int
					key    string
					flight *Flight
					leader bool
				}{index: index, key: key, flight: flight, leader: leader}
			}(i)
		}
		close(starts)
		beginWait.Wait()
		close(results)

		type beginResult struct {
			index  int
			key    string
			flight *Flight
			leader bool
		}
		begun := make([]beginResult, 0, workerCount)
		flights := make(map[string]*Flight, keyCount)
		leaders := make(map[string]*Flight, keyCount)
		for result := range results {
			begun = append(begun, beginResult(result))
			if current := flights[result.key]; current != nil {
				require.Same(t, current, result.flight)
			} else {
				flights[result.key] = result.flight
			}
			if result.leader {
				require.NotContains(t, leaders, result.key)
				leaders[result.key] = result.flight
			}
		}
		require.Len(t, flights, keyCount)
		require.Len(t, leaders, keyCount)

		flightErr := error(nil)
		if fail {
			flightErr = errors.New("flight failed")
		}
		type waitResult struct {
			canceled bool
			err      error
		}
		waitResults := make(chan waitResult, workerCount-keyCount)
		canceledWaiters := 0
		activeWaiters := 0
		for _, result := range begun {
			if result.leader {
				continue
			}
			ctx := context.Background()
			canceled := cancelMask&(uint64(1)<<result.index) != 0
			if canceled {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				canceledWaiters++
			} else {
				activeWaiters++
			}
			go func() { waitResults <- waitResult{canceled: canceled, err: group.Wait(ctx, result.flight)} }()
		}
		for range canceledWaiters {
			result := <-waitResults
			require.True(t, result.canceled)
			require.ErrorIs(t, result.err, context.Canceled)
		}
		for key, flight := range leaders {
			group.Finish(key, flight, flightErr)
		}
		for range activeWaiters {
			result := <-waitResults
			require.False(t, result.canceled)
			require.ErrorIs(t, result.err, flightErr)
		}

		for key, completed := range leaders {
			next, leader := group.Begin(key)
			require.True(t, leader)
			group.Finish(key, completed, errors.New("stale completion"))
			select {
			case <-next.done:
				t.Fatal("stale completion closed the replacement flight")
			default:
			}
			group.Finish(key, next, nil)
			require.NoError(t, group.Wait(context.Background(), next))
		}
	})
}

func FuzzParallelStreams(f *testing.F) {
	f.Add([]byte("payload"), uint8(4), uint8(0), uint64(0), uint16(3))
	f.Add([]byte("payload"), uint8(8), uint8(1), uint64(0x55), uint16(1))
	f.Add([]byte("payload"), uint8(4), uint8(2), uint64(0x0a), uint16(2))
	f.Add([]byte{}, uint8(2), uint8(3), uint64(0), uint16(1))
	f.Add([]byte("fallback"), uint8(8), uint8(2), uint64(0xffff), uint16(1))
	f.Fuzz(func(t *testing.T, input []byte, workerInput, expectedInput uint8, earlyCloseMask uint64, chunkInput uint16) {
		if len(input) > 32<<10 {
			t.Skip()
		}
		body := bytes.Clone(input)
		workerCount := int(workerInput%8) + 1
		chunkSize := int(chunkInput%1024) + 1
		expectedMode := expectedInput % 4
		if len(body) == 0 && expectedMode == 2 {
			expectedMode = 1
		}

		expectedSize := int64(len(body))
		successfulFill := true
		switch expectedMode {
		case 2:
			expectedSize--
			successfulFill = false
		case 3:
			expectedSize++
			successfulFill = false
		}
		maxObjectSize := max(int64(len(body))+1, expectedSize, 1)
		budget := proxyruntime.NewSpoolBudget(maxObjectSize * int64(workerCount))
		workDir := t.TempDir()
		spooler := NewSpooler(workDir, maxObjectSize, budget)
		lifecycle := NewLifecycle()

		type clientResult struct {
			index int
			body  []byte
			err   error
		}
		clients := make(chan clientResult, workerCount)
		done := make([]chan error, workerCount)
		stored := make([]atomic.Bool, workerCount)
		upstreams := make([]*chunkedReadCloser, workerCount)
		for i := range workerCount {
			index := i
			done[i] = make(chan error, 1)
			var declaredSize *int64
			if expectedMode != 0 {
				declaredSize = &expectedSize
			}
			upstreams[i] = &chunkedReadCloser{reader: bytes.NewReader(body), chunkSize: chunkSize}
			reader, err := StartStream(context.Background(), StreamConfig{
				Body:         upstreams[i],
				ObjectPath:   fmt.Sprintf("object-%d", i),
				Spooler:      spooler,
				Lifecycle:    lifecycle,
				ExpectedSize: declaredSize,
				StoreFn: func(_ context.Context, reader io.Reader) error {
					storedBody, readErr := io.ReadAll(reader)
					if readErr != nil {
						return readErr
					}
					if !bytes.Equal(storedBody, body) {
						return errors.New("stored body differs from upstream body")
					}
					stored[index].Store(true)
					return nil
				},
				Done: func(err error) { done[index] <- err },
			})
			require.NoError(t, err)
			go func(index int, reader io.ReadCloser) {
				if earlyCloseMask&(uint64(1)<<index) != 0 {
					buffer := make([]byte, min(chunkSize, max(len(body), 1)))
					var readErr error
					if earlyCloseMask&(uint64(1)<<(index+8)) == 0 {
						_, readErr = reader.Read(buffer)
					}
					if errors.Is(readErr, io.EOF) {
						readErr = nil
					}
					clients <- clientResult{index: index, err: errors.Join(readErr, reader.Close())}
					return
				}
				clientBody, readErr := io.ReadAll(reader)
				clients <- clientResult{index: index, body: clientBody, err: errors.Join(readErr, reader.Close())}
			}(i, reader)
		}

		for range workerCount {
			result := <-clients
			require.NoError(t, result.err)
			if earlyCloseMask&(uint64(1)<<result.index) == 0 {
				require.Equal(t, body, result.body)
			}
		}
		for i := range workerCount {
			fillErr := <-done[i]
			if successfulFill {
				require.NoError(t, fillErr)
				require.True(t, stored[i].Load())
			} else {
				require.Error(t, fillErr)
				require.False(t, stored[i].Load())
			}
		}

		closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, lifecycle.Close(closeCtx))
		for _, upstream := range upstreams {
			require.EqualValues(t, 1, upstream.closed.Load())
		}
		used, limit := budget.Usage()
		require.Zero(t, used)
		require.Equal(t, maxObjectSize*int64(workerCount), limit)
		entries, err := os.ReadDir(workDir)
		require.NoError(t, err)
		require.Empty(t, entries)
	})
}

func FuzzLifecycleParallelClose(f *testing.F) {
	f.Add([]byte{}, uint8(1))
	f.Add([]byte{0, 1, 2, 3, 4, 5}, uint8(4))
	f.Fuzz(func(t *testing.T, operations []byte, closerInput uint8) {
		if len(operations) > 32 {
			t.Skip()
		}
		closerCount := int(closerInput%4) + 1
		lifecycle := NewLifecycle()
		starts := make(chan struct{})
		type operationResult struct {
			closing bool
			err     error
		}
		results := make(chan operationResult, len(operations)+closerCount)
		var wait sync.WaitGroup
		for _, operation := range operations {
			wait.Add(1)
			go func(operation byte) {
				defer wait.Done()
				<-starts
				if operation&1 != 0 {
					err := lifecycle.Go(func(ctx context.Context) {
						if operation&2 != 0 {
							<-ctx.Done()
						} else {
							goruntime.Gosched()
						}
					})
					results <- operationResult{err: err}
					return
				}
				_, done, err := lifecycle.Begin()
				if err == nil {
					goruntime.Gosched()
					done()
					done()
				}
				results <- operationResult{err: err}
			}(operation)
		}
		for range closerCount {
			wait.Add(1)
			go func() {
				defer wait.Done()
				<-starts
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				results <- operationResult{closing: true, err: lifecycle.Close(ctx)}
			}()
		}
		close(starts)
		wait.Wait()
		close(results)
		for result := range results {
			if result.closing {
				require.NoError(t, result.err)
			} else if result.err != nil {
				require.ErrorIs(t, result.err, ErrLifecycleClosed)
			}
		}
		require.NoError(t, lifecycle.Close(context.Background()))
		require.ErrorIs(t, lifecycle.Context().Err(), context.Canceled)
		_, done, err := lifecycle.Begin()
		require.Nil(t, done)
		require.ErrorIs(t, err, ErrLifecycleClosed)
	})
}

type chunkedReadCloser struct {
	reader    *bytes.Reader
	chunkSize int
	closed    atomic.Int32
}

func (r *chunkedReadCloser) Read(buffer []byte) (int, error) {
	if len(buffer) > r.chunkSize {
		buffer = buffer[:r.chunkSize]
	}
	n, err := r.reader.Read(buffer)
	goruntime.Gosched()
	return n, err
}

func (r *chunkedReadCloser) Close() error { r.closed.Add(1); return nil }
