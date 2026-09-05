package storeio

import (
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

type pausedStreamBody struct {
	io.Reader
	started   chan struct{}
	resume    chan struct{}
	startOnce sync.Once
	closed    atomic.Int32
	release   func()
}

func (b *pausedStreamBody) Read(p []byte) (int, error) {
	b.startOnce.Do(func() { close(b.started) })
	<-b.resume
	return b.Reader.Read(p)
}

func (b *pausedStreamBody) Close() error {
	b.closed.Add(1)
	b.release()
	return nil
}

func TestStreamAbandonedFallbackReleasesResources(t *testing.T) {
	for _, writeFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "size limit", true: "write failure"}[writeFailure], func(t *testing.T) {
			gate := proxyruntime.NewUpstreamGate(proxyruntime.UpstreamGateConfig{MaxActive: 1, MaxActivePerHost: 1})
			release, err := gate.Acquire(context.Background(), "https://upstream.example", proxyruntime.AdmissionForeground)
			require.NoError(t, err)
			body := &pausedStreamBody{Reader: strings.NewReader("payload"), started: make(chan struct{}), resume: make(chan struct{}), release: release}
			limit := int64(1)
			if writeFailure {
				limit = 1024
			}
			budget := proxyruntime.NewSpoolBudget(1024)
			lifecycle := NewLifecycle()
			reader, err := StartStream(context.Background(), StreamConfig{
				Body: body, ObjectPath: "object", Spooler: NewSpooler(t.TempDir(), limit, budget), Lifecycle: lifecycle,
				StoreFn: func(context.Context, io.Reader) error { t.Error("incomplete body was published"); return nil },
			})
			require.NoError(t, err)
			<-body.started
			if writeFailure {
				require.NoError(t, reader.(*growingFileReader).spool.writer.Close())
			}
			require.NoError(t, reader.Close())
			close(body.resume)
			require.NoError(t, lifecycle.Close(context.Background()))
			require.EqualValues(t, 1, body.closed.Load())
			require.Zero(t, gate.Snapshot().Active)
			used, _ := budget.Usage()
			require.Zero(t, used)
		})
	}
}
