package storeio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

const cacheStoreTimeout = 10 * time.Minute

type StreamConfig struct {
	Body       io.ReadCloser
	ObjectPath string
	WorkDir    string
	Spooler    *Spooler
	MaxBytes   int64
	Lifecycle  *Lifecycle
	// ExpectedSize is the upstream wire length. A negative value means that
	// the upstream did not declare a length.
	ExpectedSize *int64
	StatsStart   func()
	StatsDone    func()
	VerifyFn     func(io.ReadSeeker) error
	StoreFn      func(ctx context.Context, r io.Reader) error
	Done         func(error)
}

// StartStream downloads independently of clients. Download completion is
// published before verification and storage so local cache work cannot stall
// downstream EOF or retain an upstream admission slot.
func StartStream(ctx context.Context, cfg StreamConfig) (io.ReadCloser, error) {
	if cfg.Body == nil {
		return nil, errors.New("stream body is nil")
	}
	if cfg.Lifecycle == nil {
		return nil, errors.New("stream lifecycle is nil")
	}
	if cfg.StoreFn == nil {
		return nil, errors.New("stream store function is nil")
	}
	workDir := cfg.WorkDir
	limit := cfg.MaxBytes
	var budget *proxyruntime.SpoolBudget
	if cfg.Spooler != nil {
		workDir = cfg.Spooler.workDir
		budget = cfg.Spooler.budget
		if limit <= 0 || cfg.Spooler.maxObject > 0 && limit > cfg.Spooler.maxObject {
			limit = cfg.Spooler.maxObject
		}
	}
	if limit <= 0 {
		limit = int64(^uint64(0) >> 1)
	}
	if cfg.ExpectedSize != nil && *cfg.ExpectedSize >= 0 && limit > 0 && *cfg.ExpectedSize > limit {
		return nil, ErrObjectTooLarge
	}
	reserve := limit
	spoolLimit := limit
	if cfg.ExpectedSize != nil && *cfg.ExpectedSize >= 0 {
		reserve = *cfg.ExpectedSize
		spoolLimit = *cfg.ExpectedSize
	}
	reservation, ok := budget.TryReserve(reserve)
	if !ok {
		return nil, ErrSpoolBusy
	}
	spool, err := newGrowingFile(workDir, spoolLimit, reservation.Release)
	if err != nil {
		reservation.Release()
		return nil, err
	}
	readerFile, err := os.Open(spool.path)
	if err != nil {
		_ = spool.writer.Close()
		spool.releaseProducer()
		return nil, err
	}
	spool.mu.Lock()
	spool.readers++
	spool.mu.Unlock()
	reader := &growingFileReader{spool: spool, file: readerFile}

	if cfg.StatsStart != nil {
		cfg.StatsStart()
	}
	slog.Debug("download started", "path", cfg.ObjectPath, "temp", spool.path)
	run := func() {
		if cfg.StatsDone != nil {
			defer cfg.StatsDone()
		}
		defer spool.releaseProducer()

		fallback, copyErr := spool.copyFrom(cfg.Body)
		if fallback != nil {
			spool.mu.Lock()
			spool.done = true
			spool.fallback = fallback
			spool.changed.Broadcast()
			spool.mu.Unlock()
		} else {
			closeErr := cfg.Body.Close()
			if copyErr == nil {
				copyErr = closeErr
			}
			spool.finish(copyErr)
		}

		fillErr := copyErr
		if fillErr == nil {
			fillErr = verifyAndStore(ctx, spool.path, cfg)
		}
		if fillErr != nil {
			slog.Warn("cache fill failed", "path", cfg.ObjectPath, "err", fillErr)
		} else {
			slog.Debug("download completed", "path", cfg.ObjectPath)
		}
		if cfg.Done != nil {
			cfg.Done(fillErr)
		}
	}
	if err := cfg.Lifecycle.Go(func(context.Context) { run() }); err != nil {
		if cfg.StatsDone != nil {
			cfg.StatsDone()
		}
		_ = spool.writer.Close()
		spool.finish(err)
		spool.releaseProducer()
		_ = reader.Close()
		return nil, err
	}

	return reader, nil
}

func verifyAndStore(parent context.Context, filename string, cfg StreamConfig) error {
	if cfg.ExpectedSize != nil && *cfg.ExpectedSize >= 0 {
		info, err := os.Stat(filename)
		if err != nil {
			return err
		}
		if info.Size() != *cfg.ExpectedSize {
			return fmt.Errorf("download size mismatch: got %d, want %d", info.Size(), *cfg.ExpectedSize)
		}
	}
	if cfg.VerifyFn != nil {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		err = cfg.VerifyFn(file)
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	storeCtx, cancel := context.WithTimeout(parent, cacheStoreTimeout)
	defer cancel()
	return cfg.StoreFn(storeCtx, file)
}

type growingFile struct {
	mu          sync.Mutex
	changed     *sync.Cond
	path        string
	writer      *os.File
	size        int64
	done        bool
	readErr     error
	readers     int
	producer    bool
	fallback    io.ReadCloser
	cleanupOnce sync.Once
	limit       int64
	release     func()
}

type growingFileReader struct {
	spool    *growingFile
	file     *os.File
	offset   int64
	fallback io.ReadCloser
	once     sync.Once
}

func newGrowingFile(workDir string, limit int64, release func()) (*growingFile, error) {
	if workDir == "" {
		workDir = os.TempDir()
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return nil, err
	}
	writer, err := os.CreateTemp(workDir, ".cache-proxy-tmp-stream-*")
	if err != nil {
		return nil, err
	}
	spool := &growingFile{path: writer.Name(), writer: writer, producer: true, limit: limit, release: release}
	spool.changed = sync.NewCond(&spool.mu)
	return spool, nil
}

func (f *growingFile) copyFrom(src io.ReadCloser) (io.ReadCloser, error) {
	buffer := make([]byte, 256<<10)
	for {
		n, readErr := src.Read(buffer)
		if n > 0 {
			if f.limit >= 0 && f.size > f.limit-int64(n) {
				_ = f.writer.Close()
				return &remainingBody{Reader: io.MultiReader(bytes.NewReader(bytes.Clone(buffer[:n])), src), body: src}, ErrObjectTooLarge
			}
			written, writeErr := f.writer.Write(buffer[:n])
			f.mu.Lock()
			f.size += int64(written)
			f.changed.Broadcast()
			f.mu.Unlock()
			if writeErr != nil {
				_ = f.writer.Close()
				return &remainingBody{Reader: io.MultiReader(bytes.NewReader(bytes.Clone(buffer[written:n])), src), body: src}, writeErr
			}
			if written != n {
				_ = f.writer.Close()
				return &remainingBody{Reader: io.MultiReader(bytes.NewReader(bytes.Clone(buffer[written:n])), src), body: src}, io.ErrShortWrite
			}
		}
		if readErr != nil {
			closeErr := f.writer.Close()
			if errors.Is(readErr, io.EOF) {
				return nil, closeErr
			}
			return nil, readErr
		}
	}
}

func (r *growingFileReader) Read(p []byte) (int, error) {
	if r.fallback != nil {
		return r.fallback.Read(p)
	}
	f := r.spool
	f.mu.Lock()
	for {
		if r.offset < f.size {
			available := f.size - r.offset
			if int64(len(p)) > available {
				p = p[:available]
			}
			offset := r.offset
			f.mu.Unlock()
			n, err := r.file.ReadAt(p, offset)
			f.mu.Lock()
			r.offset += int64(n)
			if n > 0 && errors.Is(err, io.EOF) {
				err = nil
			}
			f.mu.Unlock()
			return n, err
		}
		if f.fallback != nil {
			r.fallback = f.fallback
			f.fallback = nil
			f.mu.Unlock()
			return r.fallback.Read(p)
		}
		if f.done {
			f.mu.Unlock()
			if f.readErr != nil {
				return 0, f.readErr
			}
			return 0, io.EOF
		}
		f.changed.Wait()
	}
}

func (r *growingFileReader) Close() error {
	var err error
	r.once.Do(func() {
		f := r.spool
		f.mu.Lock()
		fallback := r.fallback
		if fallback == nil {
			fallback = f.fallback
			f.fallback = nil
		}
		f.readers--
		cleanup := !f.producer && f.readers == 0
		f.mu.Unlock()
		err = r.file.Close()
		if fallback != nil {
			err = errors.Join(err, fallback.Close())
		}
		if cleanup {
			f.cleanup()
		}
	})
	return err
}

func (f *growingFile) finish(readErr error) {
	f.mu.Lock()
	f.done = true
	f.readErr = readErr
	f.changed.Broadcast()
	f.mu.Unlock()
}

type remainingBody struct {
	io.Reader
	body io.Closer
}

func (r *remainingBody) Close() error {
	return r.body.Close()
}

func (f *growingFile) releaseProducer() {
	f.mu.Lock()
	f.producer = false
	cleanup := f.readers == 0
	f.mu.Unlock()
	if cleanup {
		f.cleanup()
	}
}

func (f *growingFile) cleanup() {
	f.cleanupOnce.Do(func() {
		_ = os.Remove(f.path)
		if f.release != nil {
			f.release()
		}
	})
}
