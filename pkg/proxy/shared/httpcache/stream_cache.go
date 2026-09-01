package httpcache

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"
)

const cacheStoreTimeout = 10 * time.Minute

type StreamConfig struct {
	Body       io.ReadCloser
	ObjectPath string
	Wait       *sync.WaitGroup
	StatsStart func()
	StatsDone  func()
	VerifyFn   func(io.ReadSeeker) error
	StoreFn    func(ctx context.Context, r io.Reader) error
	Done       func(error)
}

// StreamToCache downloads independently of clients. Download completion is
// published before verification and storage so local cache work cannot stall
// downstream EOF or retain an upstream admission slot.
func StreamToCache(ctx context.Context, cfg StreamConfig) (io.ReadCloser, error) {
	spool, err := startCacheStream(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return spool.Reader()
}

func startCacheStream(ctx context.Context, cfg StreamConfig) (*growingFile, error) {
	spool, err := newGrowingFile()
	if err != nil {
		return nil, err
	}

	cfg.Wait.Add(1)
	if cfg.StatsStart != nil {
		cfg.StatsStart()
	}
	slog.Debug("download started", "path", cfg.ObjectPath, "temp", spool.path)
	go func() {
		defer cfg.Wait.Done()
		if cfg.StatsDone != nil {
			defer cfg.StatsDone()
		}
		defer spool.releaseProducer()

		copyErr := spool.copyFrom(cfg.Body)
		closeErr := cfg.Body.Close()
		if copyErr == nil {
			copyErr = closeErr
		}
		spool.finish(copyErr)

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
	}()

	return spool, nil
}

func verifyAndStore(parent context.Context, filename string, cfg StreamConfig) error {
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
	cleanupOnce sync.Once
}

type growingFileReader struct {
	spool  *growingFile
	file   *os.File
	offset int64
	once   sync.Once
}

func newGrowingFile() (*growingFile, error) {
	writer, err := os.CreateTemp("", "cache-proxy-*")
	if err != nil {
		return nil, err
	}
	spool := &growingFile{path: writer.Name(), writer: writer, producer: true}
	spool.changed = sync.NewCond(&spool.mu)
	return spool, nil
}

func (f *growingFile) Reader() (io.ReadCloser, error) {
	reader, err := os.Open(f.path)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.readers++
	f.mu.Unlock()
	return &growingFileReader{spool: f, file: reader}, nil
}

func (f *growingFile) copyFrom(src io.Reader) error {
	buffer := make([]byte, 256<<10)
	for {
		n, readErr := src.Read(buffer)
		if n > 0 {
			written, writeErr := f.writer.Write(buffer[:n])
			f.mu.Lock()
			f.size += int64(written)
			f.changed.Broadcast()
			f.mu.Unlock()
			if writeErr != nil {
				_ = f.writer.Close()
				return writeErr
			}
			if written != n {
				_ = f.writer.Close()
				return io.ErrShortWrite
			}
		}
		if readErr != nil {
			closeErr := f.writer.Close()
			if errors.Is(readErr, io.EOF) {
				return closeErr
			}
			return readErr
		}
	}
}

func (r *growingFileReader) Read(p []byte) (int, error) {
	f := r.spool
	f.mu.Lock()
	defer f.mu.Unlock()
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
			return n, err
		}
		if f.done {
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
		err = r.file.Close()
		f := r.spool
		f.mu.Lock()
		f.readers--
		cleanup := !f.producer && f.readers == 0
		f.mu.Unlock()
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
	f.cleanupOnce.Do(func() { _ = os.Remove(f.path) })
}
