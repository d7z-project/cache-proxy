package cargo

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
)

func (h *handler) serveUncachedCrate(w http.ResponseWriter, request *http.Request, name, version string) {
	var registry struct {
		Download string `json:"dl"`
	}
	var crate crateState
	indexPath := cratePrefix(name) + "/" + name
	header := request.Header.Clone()
	for _, key := range []string{"If-Match", "If-None-Match", "If-Modified-Since", "If-Unmodified-Since", "Range", "If-Range"} {
		header.Del(key)
	}
	header.Set("Accept-Encoding", "identity")
	for _, object := range []struct {
		path  string
		limit int64
	}{{"config.json", maxCargoConfigSize}, {indexPath, maxSparseIndexSize}} {
		response, err := h.fetchUpstream(request.Context(), http.MethodGet, object.path, header)
		if err != nil {
			transport.WriteError(w, http.StatusBadGateway)
			return
		}
		if response.StatusCode != http.StatusOK {
			transport.WriteResponse(w, request, response, "BYPASS")
			return
		}
		spool, err := h.spooler.SpoolWithExpectedSize(request.Context(), response.Body, object.limit, response.ContentLength)
		_ = response.Body.Close()
		if err != nil {
			transport.WriteError(w, http.StatusBadGateway)
			return
		}
		if object.path == "config.json" {
			var body []byte
			body, err = io.ReadAll(spool.File)
			if err == nil {
				err = json.Unmarshal(body, &registry)
			}
		} else {
			crate, err = parseSparseIndex(spool.File, indexPath)
		}
		_ = spool.Close()
		if err != nil {
			transport.WriteError(w, http.StatusBadGateway)
			return
		}
	}
	checksum := crate.Checksums[version]
	if checksum == "" {
		http.NotFound(w, request)
		return
	}
	target, err := cargoDownloadURL(registry.Download, name, version, checksum)
	if err != nil {
		transport.WriteError(w, http.StatusBadGateway)
		return
	}
	header = request.Header.Clone()
	header.Del("Authorization")
	header.Del("Cookie")
	if target.Scheme == h.origin.Scheme && strings.EqualFold(target.Host, h.origin.Host) {
		copyCargoCredential(header, request.Header)
	}
	status, err := transport.ForwardVerified(request.Context(), h.client, target, w, request, header, h.spooler, func(reader io.ReadSeeker) error {
		digest := sha256.New()
		if _, err := io.Copy(digest, reader); err != nil {
			return err
		}
		if hex.EncodeToString(digest.Sum(nil)) != checksum {
			return errors.New("cargo crate checksum mismatch")
		}
		return nil
	})
	if err != nil && status == 0 {
		transport.WriteError(w, http.StatusBadGateway)
	}
}
