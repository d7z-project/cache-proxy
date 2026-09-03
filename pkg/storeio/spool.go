package storeio

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

var ErrSpoolBusy = errors.New("spool capacity is unavailable")
var ErrObjectTooLarge = errors.New("object exceeds cache size limit")

type Spooler struct {
	workDir   string
	maxObject int64
	budget    *proxyruntime.SpoolBudget
}

func NewSpooler(workDir string, maxObject int64, budget *proxyruntime.SpoolBudget) *Spooler {
	return &Spooler{workDir: workDir, maxObject: maxObject, budget: budget}
}

type SpoolResult struct {
	File    *os.File
	Size    int64
	SHA256  string
	release func()
}

type SpoolError struct {
	Err      error
	Consumed bool
}

func (e *SpoolError) Error() string { return e.Err.Error() }
func (e *SpoolError) Unwrap() error { return e.Err }

func SpoolBodyUntouched(err error) bool {
	var spoolError *SpoolError
	return errors.As(err, &spoolError) && !spoolError.Consumed
}

// SpoolWithExpectedSize reserves only the declared size and rejects a body
// that differs from it. A negative expected size retains unknown-size
// behavior and reserves the effective object maximum.
func (s *Spooler) SpoolWithExpectedSize(ctx context.Context, source io.Reader, maxBytes, expectedSize int64) (*SpoolResult, error) {
	if source == nil {
		return nil, errors.New("source reader is nil")
	}
	if s == nil {
		return nil, errors.New("spooler is nil")
	}
	limit := maxBytes
	if limit <= 0 || s.maxObject > 0 && limit > s.maxObject {
		limit = s.maxObject
	}
	if limit <= 0 {
		return nil, &SpoolError{Err: errors.New("spool limit must be positive")}
	}
	if expectedSize > limit {
		return nil, &SpoolError{Err: ErrObjectTooLarge}
	}
	reserve := limit
	readLimit := limit
	if expectedSize >= 0 {
		reserve = expectedSize
		readLimit = expectedSize
	}
	reservation, ok := s.budget.TryReserve(reserve)
	if !ok {
		return nil, &SpoolError{Err: ErrSpoolBusy}
	}
	release := reservation.Release
	workDir := s.workDir
	if workDir == "" {
		workDir = os.TempDir()
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		release()
		return nil, &SpoolError{Err: fmt.Errorf("create work directory: %w", err)}
	}
	file, err := os.CreateTemp(workDir, ".cache-proxy-tmp-download-*")
	if err != nil {
		release()
		return nil, &SpoolError{Err: fmt.Errorf("create spool: %w", err)}
	}
	remove := true
	defer func() {
		if remove {
			_ = file.Close()
			_ = os.Remove(file.Name())
			release()
		}
	}()

	hash := sha256.New()
	if readLimit < math.MaxInt64 {
		readLimit++
	}
	reader := io.LimitReader(source, readLimit)
	counted := &countingReader{reader: reader}
	written, err := io.Copy(io.MultiWriter(file, hash), &contextReader{ctx: ctx, reader: counted})
	if err != nil {
		return nil, &SpoolError{Err: fmt.Errorf("spool body: %w", err), Consumed: counted.read}
	}
	if written >= readLimit && readLimit == math.MaxInt64 || written > limit || expectedSize >= 0 && written > expectedSize {
		return nil, &SpoolError{Err: fmt.Errorf("%w: %d bytes", ErrObjectTooLarge, limit), Consumed: true}
	}
	if expectedSize >= 0 && written != expectedSize {
		return nil, &SpoolError{Err: fmt.Errorf("spool size mismatch: got %d, want %d", written, expectedSize), Consumed: true}
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, &SpoolError{Err: fmt.Errorf("rewind spool: %w", err), Consumed: true}
	}
	remove = false
	return &SpoolResult{File: file, Size: written, SHA256: hex.EncodeToString(hash.Sum(nil)), release: release}, nil
}

func (s *SpoolResult) Close() error {
	if s == nil {
		return nil
	}
	var err error
	if s.File != nil {
		name := s.File.Name()
		err = errors.Join(s.File.Close(), os.Remove(name))
		s.File = nil
	}
	if s.release != nil {
		s.release()
		s.release = nil
	}
	return err
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

type countingReader struct {
	reader io.Reader
	count  int64
	read   bool
}

func (r *countingReader) Read(p []byte) (int, error) {
	r.read = true
	n, err := r.reader.Read(p)
	r.count += int64(n)
	return n, err
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}
