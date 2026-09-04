package flatpak

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/foundriesio/ostreeuploader/pkg/gvariant"
)

const (
	summaryIndexFormat          = "(a{s(ayaaya{sv})}a{sv})"
	summaryIndexMaxBytes        = int64(16 << 20)
	indexedSummaryMaxBytes      = int64(512 << 20)
	summaryIndexMaxEntries      = 8192
	summaryIndexMaxHistory      = 65536
	summaryIndexMaxMetadataKeys = 65536
)

func parseSummaryIndexDigest(reader io.Reader, size int64) (string, error) {
	if size < 0 || size > summaryIndexMaxBytes {
		return "", fmt.Errorf("flatpak summary index has invalid size %d", size)
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(reader, data); err != nil {
		return "", fmt.Errorf("read flatpak summary index: %w", err)
	}
	index, err := gvariant.New(data, summaryIndexFormat)
	if err != nil {
		return "", fmt.Errorf("parse flatpak summary index: %w", err)
	}

	entries := index.Child(0)
	entryCount := entries.Len()
	if err := entries.Err(); err != nil {
		return "", fmt.Errorf("parse flatpak summary index entries: %w", err)
	}
	if entryCount > summaryIndexMaxEntries {
		return "", fmt.Errorf("flatpak summary index has too many entries: %d", entryCount)
	}

	previousName := ""
	historyCount := 0
	metadataCount := 0
	for i := 0; i < entryCount; i++ {
		entry := entries.At(i)
		nameValue := entry.Child(0)
		name, err := summaryIndexString(nameValue)
		if err != nil {
			return "", fmt.Errorf("flatpak summary index entry %d name: %w", i, err)
		}
		if name == "" {
			return "", fmt.Errorf("flatpak summary index entry %d has empty name", i)
		}
		if i > 0 && name <= previousName {
			return "", errors.New("flatpak summary index entries are not in canonical order")
		}
		previousName = name
		value := entry.Child(1)
		checksumValue := value.Child(0)
		checksum := checksumValue.Bytes()
		if err := checksumValue.Err(); err != nil {
			return "", fmt.Errorf("flatpak summary index entry %q checksum: %w", name, err)
		}
		if len(checksum) != sha256.Size {
			return "", fmt.Errorf("flatpak summary index entry %q has invalid checksum length %d", name, len(checksum))
		}
		history := value.Child(1)
		historyLength := history.Len()
		if err := history.Err(); err != nil {
			return "", fmt.Errorf("flatpak summary index entry %q history: %w", name, err)
		}
		if historyLength > summaryIndexMaxHistory-historyCount {
			return "", errors.New("flatpak summary index has too many history entries")
		}
		historyCount += historyLength
		for j := 0; j < historyLength; j++ {
			previousValue := history.At(j)
			previous := previousValue.Bytes()
			if err := previousValue.Err(); err != nil {
				return "", fmt.Errorf("flatpak summary index entry %q history: %w", name, err)
			}
			if len(previous) != sha256.Size {
				return "", fmt.Errorf("flatpak summary index entry %q has invalid history checksum length %d", name, len(previous))
			}
		}

		count, err := validateSummaryIndexMetadata(value.Child(2))
		if err != nil {
			return "", fmt.Errorf("flatpak summary index entry %q metadata: %w", name, err)
		}
		metadataCount += count
		if metadataCount > summaryIndexMaxMetadataKeys {
			return "", errors.New("flatpak summary index has too many metadata keys")
		}
	}
	count, err := validateSummaryIndexMetadata(index.Child(1))
	if err != nil {
		return "", fmt.Errorf("flatpak summary index metadata: %w", err)
	}
	if metadataCount+count > summaryIndexMaxMetadataKeys {
		return "", errors.New("flatpak summary index has too many metadata keys")
	}

	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func summaryIndexString(value *gvariant.Value) (string, error) {
	raw := value.Raw()
	if err := value.Err(); err != nil {
		return "", err
	}
	if len(raw) == 0 || raw[len(raw)-1] != 0 || bytes.IndexByte(raw[:len(raw)-1], 0) >= 0 || !utf8.Valid(raw[:len(raw)-1]) {
		return "", errors.New("invalid GVariant string")
	}
	return string(raw[:len(raw)-1]), nil
}

func validateSummaryIndexMetadata(metadata *gvariant.Value) (int, error) {
	count := metadata.Len()
	if err := metadata.Err(); err != nil {
		return 0, err
	}
	if count > summaryIndexMaxMetadataKeys {
		return 0, errors.New("too many metadata keys")
	}
	for i := 0; i < count; i++ {
		entry := metadata.At(i)
		if _, err := summaryIndexString(entry.Child(0)); err != nil {
			return 0, err
		}
		variant := entry.Child(1).Variant()
		if err := variant.Err(); err != nil {
			return 0, err
		}
	}
	return count, nil
}

func verifyIndexedSummary(reader io.Reader, digest string, maxBytes int64) error {
	compressed, err := gzip.NewReader(reader)
	if err != nil {
		return fmt.Errorf("open flatpak indexed summary %s: %w", digest, err)
	}
	hash := sha256.New()
	size, copyErr := io.Copy(hash, io.LimitReader(compressed, maxBytes+1))
	closeErr := compressed.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return fmt.Errorf("read flatpak indexed summary %s: %w", digest, err)
	}
	if size > maxBytes {
		return fmt.Errorf("flatpak indexed summary %s exceeds size limit", digest)
	}
	if actual := hex.EncodeToString(hash.Sum(nil)); actual != digest {
		return fmt.Errorf("flatpak indexed summary %s checksum mismatch", digest)
	}
	return nil
}
