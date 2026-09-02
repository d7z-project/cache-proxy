package oci

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
	"unicode"

	"gopkg.d7z.net/cache-proxy/pkg/utils"
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

func cacheSupportsRequestUserAgent(client *utils.HTTPClientWrapper, request *http.Request, options map[string]string) bool {
	if client.UserAgentConfigured || !utils.IsBrowserRequest(request) {
		return true
	}
	return options[userAgentReviewedOption] == "true" && !utils.VariesByUserAgent(options["vary"])
}
