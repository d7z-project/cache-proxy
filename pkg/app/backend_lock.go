package app

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

type backendLock struct {
	file *os.File
}

func lockBackend(backend string) (*backendLock, error) {
	file, err := os.OpenFile(filepath.Join(backend, "backend.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open backend lock: %w", err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, errors.New("backend is already opened by another cache-proxy process")
		}
		return nil, fmt.Errorf("lock backend: %w", err)
	}
	return &backendLock{file: file}, nil
}

func (l *backendLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := unix.Flock(int(l.file.Fd()), unix.LOCK_UN)
	err = errors.Join(err, l.file.Close())
	l.file = nil
	return err
}
