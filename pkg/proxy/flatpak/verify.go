package flatpak

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

func (h *Handler) verifyCacheObject(_ *http.Request, route httpcache.Route, reader io.ReadSeeker) error {
	if isObjectPath(route.UpstreamPath) {
		if !h.verifyObjects {
			return nil
		}
		return verifyOSTreeObject(route.UpstreamPath, reader)
	}
	return nil
}

func verifyOSTreeObject(cleanPath string, reader io.ReadSeeker) error {
	expected, ext, ok := objectDigestFromPath(cleanPath)
	if !ok {
		return fmt.Errorf("invalid flatpak object path %s", cleanPath)
	}
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind flatpak object %s: %w", cleanPath, err)
	}

	sum := sha256.New()
	if ext == ".filez" {
		gzipReader, err := gzip.NewReader(reader)
		if err != nil {
			return fmt.Errorf("open flatpak filez object %s: %w", cleanPath, err)
		}
		if _, err := io.Copy(sum, gzipReader); err != nil {
			_ = gzipReader.Close()
			return fmt.Errorf("hash flatpak filez object %s: %w", cleanPath, err)
		}
		if err := gzipReader.Close(); err != nil {
			return fmt.Errorf("close flatpak filez object %s: %w", cleanPath, err)
		}
	} else {
		if _, err := io.Copy(sum, reader); err != nil {
			return fmt.Errorf("hash flatpak metadata object %s: %w", cleanPath, err)
		}
	}

	actual := hex.EncodeToString(sum.Sum(nil))
	if actual != expected {
		return fmt.Errorf("flatpak object checksum mismatch for %s: got %s want %s", cleanPath, actual, expected)
	}
	return nil
}
