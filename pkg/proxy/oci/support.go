package oci

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
)

const userAgentReviewedOption = "user-agent-reviewed"

func safePath(value string) bool {
	if value == "" || strings.HasPrefix(value, "/") || strings.Contains(value, "\\") || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return false
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}

func hashKey(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func responseBytes(headers map[string]string) uint64 {
	value, _ := strconv.ParseUint(headers["Content-Length"], 10, 64)
	return value
}

func cacheSupportsRequestUserAgent(client *transport.UpstreamHTTPClient, request *http.Request, options map[string]string) bool {
	if client.UserAgentConfigured || !transport.IsBrowserRequest(request) {
		return true
	}
	return options[userAgentReviewedOption] == "true" && !transport.VariesByUserAgent(options["vary"])
}

const referenceLockShardCount = 4096

type referenceLocks struct {
	locks [referenceLockShardCount]sync.RWMutex
}

func (g *referenceLocks) Get(key string) *sync.RWMutex {
	const offset32 = 2166136261
	const prime32 = 16777619
	hash := uint32(offset32)
	for i := range len(key) {
		hash ^= uint32(key[i])
		hash *= prime32
	}
	return &g.locks[hash%referenceLockShardCount]
}
