package storeio

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
)

// CaptureResponse forwards an upstream response immediately while independently
// capturing a complete copy. Once headers are written, capture failures are
// returned only to the caller and never replace the upstream status downstream.
func CaptureResponse(ctx context.Context, w http.ResponseWriter, response *http.Response, spooler *Spooler, maxBytes int64, cacheStatus string) (*SpoolResult, error) {
	if response == nil || response.Body == nil {
		return nil, &SpoolError{Err: errors.New("capture response body is nil")}
	}
	if spooler == nil {
		return nil, &SpoolError{Err: errors.New("capture spooler is nil")}
	}
	limit := maxBytes
	if limit <= 0 || spooler.maxObject > 0 && limit > spooler.maxObject {
		limit = spooler.maxObject
	}
	if limit <= 0 {
		return nil, &SpoolError{Err: errors.New("capture limit must be positive")}
	}
	if response.ContentLength > limit {
		return nil, &SpoolError{Err: ErrObjectTooLarge}
	}
	reserve := limit
	if response.ContentLength >= 0 {
		reserve = response.ContentLength
	}
	reservation, ok := spooler.budget.TryReserve(reserve)
	if !ok {
		return nil, &SpoolError{Err: ErrSpoolBusy}
	}
	workDir := spooler.workDir
	if workDir == "" {
		workDir = os.TempDir()
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		reservation.Release()
		return nil, &SpoolError{Err: fmt.Errorf("create work directory: %w", err)}
	}
	file, err := os.CreateTemp(workDir, ".cache-proxy-tmp-capture-*")
	if err != nil {
		reservation.Release()
		return nil, &SpoolError{Err: fmt.Errorf("create capture: %w", err)}
	}
	remove := true
	defer func() {
		if remove {
			_ = file.Close()
			_ = os.Remove(file.Name())
			reservation.Release()
		}
	}()

	proxyruntime.CopyEndToEndHeaders(w.Header(), response.Header)
	w.Header().Set("X-Cache", cacheStatus)
	w.WriteHeader(response.StatusCode)

	hash := sha256.New()
	buffer := make([]byte, 256<<10)
	var size int64
	client := io.Writer(w)
	var captureErr error
	for {
		if err := ctx.Err(); err != nil {
			captureErr = err
			break
		}
		n, readErr := response.Body.Read(buffer)
		if n > 0 {
			if client != nil {
				if written, writeErr := client.Write(buffer[:n]); writeErr != nil || written != n {
					client = nil
				}
			}
			if captureErr == nil {
				if response.ContentLength >= 0 && (size > response.ContentLength || int64(n) > response.ContentLength-size) {
					captureErr = fmt.Errorf("capture size exceeds declared length %d", response.ContentLength)
				} else if size > limit-int64(n) {
					captureErr = ErrObjectTooLarge
				} else if written, writeErr := io.MultiWriter(file, hash).Write(buffer[:n]); writeErr != nil || written != n {
					if writeErr == nil {
						writeErr = io.ErrShortWrite
					}
					captureErr = writeErr
				} else {
					size += int64(n)
				}
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) && captureErr == nil {
				captureErr = readErr
			}
			break
		}
	}
	if captureErr != nil {
		return nil, &SpoolError{Err: fmt.Errorf("capture response: %w", captureErr), Consumed: true}
	}
	if response.ContentLength >= 0 && size != response.ContentLength {
		return nil, &SpoolError{Err: fmt.Errorf("capture size mismatch: got %d, want %d", size, response.ContentLength), Consumed: true}
	}
	if err := file.Sync(); err != nil {
		return nil, &SpoolError{Err: fmt.Errorf("sync capture: %w", err), Consumed: true}
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, &SpoolError{Err: fmt.Errorf("rewind capture: %w", err), Consumed: true}
	}
	remove = false
	return &SpoolResult{File: file, Size: size, SHA256: hex.EncodeToString(hash.Sum(nil)), release: reservation.Release}, nil
}
