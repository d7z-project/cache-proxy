package storeio

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

func TestWriteJSONCommitsStrictState(t *testing.T) {
	root := t.TempDir()
	type state struct {
		Version int `json:"version"`
	}
	require.NoError(t, WriteJSON(root, "refs/state.json", state{Version: 3}))
	var loaded state
	require.NoError(t, ReadJSON(root, "refs/state.json", &loaded))
	require.Equal(t, 3, loaded.Version)
	require.NoError(t, os.WriteFile(root+"/refs/state.json", []byte(`{"version":3,"unknown":true}`), 0o600))
	require.Error(t, ReadJSON(root, "refs/state.json", &loaded))
}

func TestSpoolBoundsAndHashes(t *testing.T) {
	result, err := NewSpooler(t.TempDir(), 4, nil).SpoolWithExpectedSize(context.Background(), strings.NewReader("body"), 4, -1)
	require.NoError(t, err)
	require.Equal(t, int64(4), result.Size)
	require.Equal(t, "230d8358dc8e8890b4c58deeb62912ee2f20357ae92a5cc861b98e68fe31acb5", result.SHA256)
	require.NoError(t, result.Close())
	_, err = NewSpooler(t.TempDir(), 4, nil).SpoolWithExpectedSize(context.Background(), strings.NewReader("oversized"), 4, -1)
	require.Error(t, err)
}

func TestSpoolReportsUntouchedBody(t *testing.T) {
	workFile := t.TempDir() + "/not-a-directory"
	require.NoError(t, os.WriteFile(workFile, []byte("x"), 0o600))
	source := &countingReader{reader: strings.NewReader("body")}
	_, err := NewSpooler(workFile, 10, nil).SpoolWithExpectedSize(context.Background(), source, 10, -1)
	require.Error(t, err)
	require.True(t, SpoolBodyUntouched(err))
	require.Zero(t, source.count)
}

func TestFlightGroupJoinsSameKey(t *testing.T) {
	var runs atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	group := &FlightGroup{}
	first := make(chan error, 1)
	go func() {
		first <- group.Do(context.Background(), "object", func() error {
			runs.Add(1)
			close(started)
			<-release
			return errors.New("failed")
		})
	}()
	<-started
	second := make(chan error, 1)
	go func() { second <- group.Do(context.Background(), "object", func() error { runs.Add(1); return nil }) }()
	time.Sleep(10 * time.Millisecond)
	close(release)
	require.EqualError(t, <-first, "failed")
	require.EqualError(t, <-second, "failed")
	require.Equal(t, int32(1), runs.Load())
}

func TestStreamEOFDoesNotWaitForCachePublication(t *testing.T) {
	group := &FlightGroup{}
	flight, leader := group.Begin("object")
	require.True(t, leader)
	publicationStarted := make(chan struct{})
	releasePublication := make(chan struct{})
	lifecycle := NewLifecycle()
	reader, err := StartStream(context.Background(), StreamConfig{
		Body:       io.NopCloser(strings.NewReader("body")),
		ObjectPath: "object",
		WorkDir:    t.TempDir(),
		Lifecycle:  lifecycle,
		StoreFn: func(context.Context, io.Reader) error {
			close(publicationStarted)
			<-releasePublication
			return nil
		},
		Done: func(err error) { group.Finish("object", flight, err) },
	})
	require.NoError(t, err)
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "body", string(body))
	require.NoError(t, reader.Close())
	<-publicationStarted

	joined, joinedLeader := group.Begin("object")
	require.False(t, joinedLeader)
	joinedDone := make(chan error, 1)
	go func() { joinedDone <- group.Wait(context.Background(), joined) }()
	select {
	case <-joinedDone:
		t.Fatal("joined request completed before publication")
	case <-time.After(20 * time.Millisecond):
	}
	close(releasePublication)
	require.NoError(t, <-joinedDone)
	require.NoError(t, lifecycle.Close(context.Background()))
}

func TestStartStreamFastProducerKeepsReaderAlive(t *testing.T) {
	workDir := t.TempDir()
	lifecycle := NewLifecycle()
	for range 100 {
		reader, err := StartStream(context.Background(), StreamConfig{
			Body: io.NopCloser(strings.NewReader("body")), WorkDir: workDir, Lifecycle: lifecycle,
			StoreFn: func(_ context.Context, body io.Reader) error {
				_, err := io.Copy(io.Discard, body)
				return err
			},
		})
		require.NoError(t, err)
		body, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, "body", string(body))
		require.NoError(t, reader.Close())
	}
	require.NoError(t, lifecycle.Close(context.Background()))
	entries, err := os.ReadDir(workDir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestStartStreamRejectsDeclaredLengthMismatch(t *testing.T) {
	lifecycle := NewLifecycle()
	expected := int64(5)
	done := make(chan error, 1)
	stored := atomic.Bool{}
	reader, err := StartStream(context.Background(), StreamConfig{
		Body: io.NopCloser(strings.NewReader("body")), WorkDir: t.TempDir(), Lifecycle: lifecycle,
		ExpectedSize: &expected,
		StoreFn: func(context.Context, io.Reader) error {
			stored.Store(true)
			return nil
		},
		Done: func(err error) { done <- err },
	})
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.ErrorContains(t, <-done, "download size mismatch")
	require.False(t, stored.Load())
	require.NoError(t, lifecycle.Close(context.Background()))
}

func TestStartStreamBudgetExhaustionDoesNotConsumeBodyAndReleasesAfterSuccess(t *testing.T) {
	budget := proxyruntime.NewSpoolBudget(4)
	held, ok := budget.TryReserve(4)
	require.True(t, ok)
	lifecycle := NewLifecycle()
	source := &countingReader{reader: strings.NewReader("body")}
	expected := int64(4)
	_, err := StartStream(context.Background(), StreamConfig{
		Body: io.NopCloser(source), Spooler: NewSpooler(t.TempDir(), 4, budget), Lifecycle: lifecycle, ExpectedSize: &expected,
		StoreFn: func(context.Context, io.Reader) error { return nil },
	})
	require.ErrorIs(t, err, ErrSpoolBusy)
	require.Zero(t, source.count)
	held.Release()

	done := make(chan error, 1)
	reader, err := StartStream(context.Background(), StreamConfig{
		Body: io.NopCloser(strings.NewReader("body")), Spooler: NewSpooler(t.TempDir(), 4, budget), Lifecycle: lifecycle, ExpectedSize: &expected,
		StoreFn: func(_ context.Context, body io.Reader) error {
			_, err := io.Copy(io.Discard, body)
			return err
		},
		Done: func(err error) { done <- err },
	})
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.NoError(t, <-done)
	require.NoError(t, lifecycle.Close(context.Background()))
	used, _ := budget.Usage()
	require.Zero(t, used)
}

func TestSpoolWithExpectedSizeReservesDeclaredBytes(t *testing.T) {
	budget := proxyruntime.NewSpoolBudget(4)
	spooler := NewSpooler(t.TempDir(), 16, budget)
	spool, err := spooler.SpoolWithExpectedSize(context.Background(), strings.NewReader("body"), 16, 4)
	require.NoError(t, err)
	used, _ := budget.Usage()
	require.Equal(t, int64(4), used)
	source := &countingReader{reader: strings.NewReader("body")}
	_, err = spooler.SpoolWithExpectedSize(context.Background(), source, 16, -1)
	require.ErrorIs(t, err, ErrSpoolBusy)
	require.Zero(t, source.count)
	require.NoError(t, spool.Close())
	used, _ = budget.Usage()
	require.Zero(t, used)
}

func TestSpoolWithExpectedSizeRejectsMismatchAndReleasesBudget(t *testing.T) {
	budget := proxyruntime.NewSpoolBudget(8)
	spooler := NewSpooler(t.TempDir(), 8, budget)
	_, err := spooler.SpoolWithExpectedSize(context.Background(), strings.NewReader("extra"), 8, 4)
	require.ErrorIs(t, err, ErrObjectTooLarge)
	used, _ := budget.Usage()
	require.Zero(t, used)

	_, err = spooler.SpoolWithExpectedSize(context.Background(), strings.NewReader("x"), 8, 4)
	require.ErrorContains(t, err, "spool size mismatch")
	used, _ = budget.Usage()
	require.Zero(t, used)
}

func TestSpoolSizeMismatchAfterEmptyReadIsNotUntouched(t *testing.T) {
	spooler := NewSpooler(t.TempDir(), 8, proxyruntime.NewSpoolBudget(8))
	_, err := spooler.SpoolWithExpectedSize(context.Background(), strings.NewReader(""), 8, 1)
	require.Error(t, err)
	require.False(t, SpoolBodyUntouched(err))
}

func TestStartStreamBypassesCacheWhenBodyExceedsDeclaredLength(t *testing.T) {
	lifecycle := NewLifecycle()
	expected := int64(1)
	done := make(chan error, 1)
	stored := atomic.Bool{}
	reader, err := StartStream(context.Background(), StreamConfig{
		Body: io.NopCloser(strings.NewReader("body")), Spooler: NewSpooler(t.TempDir(), 8, proxyruntime.NewSpoolBudget(8)), Lifecycle: lifecycle, ExpectedSize: &expected,
		StoreFn: func(context.Context, io.Reader) error { stored.Store(true); return nil }, Done: func(err error) { done <- err },
	})
	require.NoError(t, err)
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "body", string(body))
	require.NoError(t, reader.Close())
	require.ErrorIs(t, <-done, ErrObjectTooLarge)
	require.False(t, stored.Load())
	require.NoError(t, lifecycle.Close(context.Background()))
}

func TestStartStreamFallsBackToUpstreamAfterSpoolWriteFailure(t *testing.T) {
	lifecycle := NewLifecycle()
	source, writer := io.Pipe()
	done := make(chan error, 1)
	stored := atomic.Bool{}
	reader, err := StartStream(context.Background(), StreamConfig{
		Body: source, WorkDir: t.TempDir(), Lifecycle: lifecycle,
		StoreFn: func(context.Context, io.Reader) error { stored.Store(true); return nil },
		Done:    func(err error) { done <- err },
	})
	require.NoError(t, err)
	growing := reader.(*growingFileReader)
	require.NoError(t, growing.spool.writer.Close())
	go func() {
		_, _ = io.WriteString(writer, "complete upstream body")
		_ = writer.Close()
	}()

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "complete upstream body", string(body))
	require.NoError(t, reader.Close())
	require.Error(t, <-done)
	require.False(t, stored.Load())
	require.NoError(t, lifecycle.Close(context.Background()))
}

func TestStartStreamRejectsIncompleteConfigurationBeforeReading(t *testing.T) {
	lifecycle := NewLifecycle()
	source := &countingReader{reader: strings.NewReader("body")}
	_, err := StartStream(context.Background(), StreamConfig{
		Body: io.NopCloser(source), WorkDir: t.TempDir(), Lifecycle: lifecycle,
	})
	require.EqualError(t, err, "stream store function is nil")
	require.Zero(t, source.count)
	require.NoError(t, lifecycle.Close(context.Background()))

	_, err = StartStream(context.Background(), StreamConfig{StoreFn: func(context.Context, io.Reader) error { return nil }})
	require.EqualError(t, err, "stream body is nil")
}
