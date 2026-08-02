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

// StreamToCache downloads independently of the client while exposing a reader
// over the growing temporary file. A slow or disconnected client therefore
// cannot stall cache publication.
func StreamToCache(ctx context.Context, cfg StreamConfig) (io.ReadCloser, error) {
	spool, err := newGrowingFile()
	if err != nil {
		_ = cfg.Body.Close()
		return nil, err
	}

	cfg.Wait.Add(1)
	if cfg.StatsStart != nil {
		cfg.StatsStart()
	}
	slog.Debug("download started", "path", cfg.ObjectPath, "temp", spool.path)
	go func() {
		defer cfg.Wait.Done()
		defer cfg.Body.Close()
		if cfg.StatsDone != nil {
			defer cfg.StatsDone()
		}

		copyErr := spool.copyFrom(cfg.Body)
		fillErr := copyErr
		if copyErr == nil {
			fillErr = verifyAndStore(ctx, spool.path, cfg)
		}
		if fillErr != nil {
			slog.Warn("cache fill failed", "path", cfg.ObjectPath, "err", fillErr)
		} else {
			slog.Debug("download completed", "path", cfg.ObjectPath)
		}
		spool.finish(copyErr)
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
	defer file.Close()
	storeCtx, cancel := context.WithTimeout(parent, cacheStoreTimeout)
	defer cancel()
	return cfg.StoreFn(storeCtx, file)
}

type growingFile struct {
	mu          sync.Mutex
	changed     *sync.Cond
	path        string
	reader      *os.File
	writer      *os.File
	size        int64
	offset      int64
	done        bool
	closed      bool
	readErr     error
	cleanupOnce sync.Once
}

func newGrowingFile() (*growingFile, error) {
	writer, err := os.CreateTemp("", "cache-proxy-*")
	if err != nil {
		return nil, err
	}
	reader, err := os.Open(writer.Name())
	if err != nil {
		_ = writer.Close()
		_ = os.Remove(writer.Name())
		return nil, err
	}
	file := &growingFile{path: writer.Name(), reader: reader, writer: writer}
	file.changed = sync.NewCond(&file.mu)
	return file, nil
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

func (f *growingFile) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for {
		if f.closed {
			return 0, os.ErrClosed
		}
		if f.offset < f.size {
			available := f.size - f.offset
			if int64(len(p)) > available {
				p = p[:available]
			}
			offset := f.offset
			f.mu.Unlock()
			n, err := f.reader.ReadAt(p, offset)
			f.mu.Lock()
			f.offset += int64(n)
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

func (f *growingFile) Close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	f.closed = true
	done := f.done
	f.changed.Broadcast()
	f.mu.Unlock()
	err := f.reader.Close()
	if done {
		f.cleanup()
	}
	return err
}

func (f *growingFile) finish(readErr error) {
	f.mu.Lock()
	f.done = true
	f.readErr = readErr
	closed := f.closed
	f.changed.Broadcast()
	f.mu.Unlock()
	if closed {
		f.cleanup()
	}
}

func (f *growingFile) cleanup() {
	f.cleanupOnce.Do(func() {
		_ = os.Remove(f.path)
	})
}
