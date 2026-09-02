package storeio

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

type captureResponseWriter struct {
	mu          sync.Mutex
	header      http.Header
	status      int
	body        bytes.Buffer
	headerReady chan struct{}
	failWrites  bool
	once        sync.Once
}

func newCaptureResponseWriter(failWrites bool) *captureResponseWriter {
	return &captureResponseWriter{header: make(http.Header), headerReady: make(chan struct{}), failWrites: failWrites}
}

func (w *captureResponseWriter) Header() http.Header { return w.header }

func (w *captureResponseWriter) WriteHeader(status int) {
	w.mu.Lock()
	w.status = status
	w.mu.Unlock()
	w.once.Do(func() { close(w.headerReady) })
}

func (w *captureResponseWriter) Write(body []byte) (int, error) {
	if w.failWrites {
		return 0, errors.New("client disconnected")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.body.Write(body)
}

func TestCaptureResponseEmitsHeadersBeforeEOFAndSurvivesClientFailure(t *testing.T) {
	upstreamReader, upstreamWriter := io.Pipe()
	response := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"ETag": {`"v1"`}}, Body: upstreamReader, ContentLength: -1}
	budget := proxyruntime.NewSpoolBudget(32)
	writer := newCaptureResponseWriter(true)
	type result struct {
		spool *SpoolResult
		err   error
	}
	done := make(chan result, 1)
	go func() {
		spool, err := CaptureResponse(context.Background(), writer, response, NewSpooler(t.TempDir(), 32, budget), 32, "MISS")
		done <- result{spool: spool, err: err}
	}()

	<-writer.headerReady
	writer.mu.Lock()
	require.Equal(t, http.StatusOK, writer.status)
	writer.mu.Unlock()
	require.Equal(t, `"v1"`, writer.Header().Get("ETag"))
	require.Equal(t, int64(32), func() int64 { used, _ := budget.Usage(); return used }())

	_, err := io.WriteString(upstreamWriter, "complete metadata")
	require.NoError(t, err)
	require.NoError(t, upstreamWriter.Close())
	captured := <-done
	require.NoError(t, captured.err)
	body, err := io.ReadAll(captured.spool.File)
	require.NoError(t, err)
	require.Equal(t, "complete metadata", string(body))
	require.NoError(t, captured.spool.Close())
	used, _ := budget.Usage()
	require.Zero(t, used)
}

func TestCaptureResponseEmitsBodyBeforeUpstreamEOF(t *testing.T) {
	upstreamReader, upstreamWriter := io.Pipe()
	response := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: upstreamReader, ContentLength: -1}
	writer := newCaptureResponseWriter(false)
	type result struct {
		spool *SpoolResult
		err   error
	}
	done := make(chan result, 1)
	go func() {
		spool, err := CaptureResponse(context.Background(), writer, response, NewSpooler(t.TempDir(), 32, nil), 32, "MISS")
		done <- result{spool: spool, err: err}
	}()

	<-writer.headerReady
	_, err := io.WriteString(upstreamWriter, "prefix")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		writer.mu.Lock()
		defer writer.mu.Unlock()
		return writer.body.String() == "prefix"
	}, time.Second, time.Millisecond)
	select {
	case <-done:
		t.Fatal("capture completed before upstream EOF")
	default:
	}
	_, err = io.WriteString(upstreamWriter, " suffix")
	require.NoError(t, err)
	require.NoError(t, upstreamWriter.Close())
	captured := <-done
	require.NoError(t, captured.err)
	require.NoError(t, captured.spool.Close())
}

func TestCaptureResponseBudgetFailureLeavesBodyUntouched(t *testing.T) {
	budget := proxyruntime.NewSpoolBudget(4)
	held, ok := budget.TryReserve(4)
	require.True(t, ok)
	source := &countingReader{reader: bytes.NewBufferString("body")}
	response := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(source), ContentLength: -1}
	_, err := CaptureResponse(context.Background(), newCaptureResponseWriter(false), response, NewSpooler(t.TempDir(), 4, budget), 4, "MISS")
	require.ErrorIs(t, err, ErrSpoolBusy)
	require.True(t, SpoolBodyUntouched(err))
	require.Zero(t, source.count)
	held.Release()
}

func TestCaptureResponseOversizeStillFinishesDownstream(t *testing.T) {
	budget := proxyruntime.NewSpoolBudget(4)
	writer := newCaptureResponseWriter(false)
	response := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewBufferString("oversized")), ContentLength: -1}
	spool, err := CaptureResponse(context.Background(), writer, response, NewSpooler(t.TempDir(), 4, budget), 4, "MISS")
	require.Nil(t, spool)
	require.ErrorIs(t, err, ErrObjectTooLarge)
	writer.mu.Lock()
	require.Equal(t, "oversized", writer.body.String())
	writer.mu.Unlock()
	used, _ := budget.Usage()
	require.Zero(t, used)
}

func TestCaptureResponseDeclaredLengthCannotExceedReservation(t *testing.T) {
	budget := proxyruntime.NewSpoolBudget(8)
	writer := newCaptureResponseWriter(false)
	response := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewBufferString("body")), ContentLength: 1}
	spool, err := CaptureResponse(context.Background(), writer, response, NewSpooler(t.TempDir(), 8, budget), 8, "MISS")
	require.Nil(t, spool)
	require.ErrorContains(t, err, "declared length")
	writer.mu.Lock()
	require.Equal(t, "body", writer.body.String())
	writer.mu.Unlock()
	used, _ := budget.Usage()
	require.Zero(t, used)
}

func TestSpoolMaxInt64DoesNotOverflowLimit(t *testing.T) {
	spool, err := NewSpooler(t.TempDir(), int64(^uint64(0)>>1), nil).SpoolWithExpectedSize(context.Background(), bytes.NewBufferString("x"), int64(^uint64(0)>>1), -1)
	require.NoError(t, err)
	require.Equal(t, int64(1), spool.Size)
	require.NoError(t, spool.Close())
}
