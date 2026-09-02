package storeio

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func WriteJSON(root, name string, value any) error {
	cleaned, err := CleanRelative(name)
	if err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	data = append(data, '\n')
	destination := filepath.Join(root, filepath.FromSlash(cleaned))
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(destination), ".cache-proxy-tmp-state-*")
	if err != nil {
		return fmt.Errorf("create temporary state: %w", err)
	}
	temporaryName := temporary.Name()
	defer func() { _ = os.Remove(temporaryName) }()
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write state: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close state: %w", err)
	}
	if err := os.Rename(temporaryName, destination); err != nil {
		return fmt.Errorf("commit state: %w", err)
	}
	directory, err := os.Open(filepath.Dir(destination))
	if err != nil {
		return fmt.Errorf("open state directory: %w", err)
	}
	err = errors.Join(directory.Sync(), directory.Close())
	if err != nil {
		return fmt.Errorf("sync state directory: %w", err)
	}
	return nil
}

func ReadJSON(root, name string, target any) error {
	cleaned, err := CleanRelative(name)
	if err != nil {
		return err
	}
	file, err := os.Open(filepath.Join(root, filepath.FromSlash(cleaned)))
	if err != nil {
		return err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil {
		return err
	}
	if len(data) > 1<<20 {
		return errors.New("state exceeds 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode state: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("state contains multiple JSON values")
		}
		return fmt.Errorf("decode trailing state: %w", err)
	}
	return nil
}
