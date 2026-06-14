package proxy

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func (h *Handler) retryOCIChallenge(ctx context.Context, method, targetURL string, headers map[string]string, response *http.Response) (*http.Response, error) {
	challenge, ok := parseOCIChallenge(response.Header.Get("WWW-Authenticate"))
	if !ok {
		return nil, nil
	}
	_ = response.Body.Close()
	var auth string
	switch strings.ToLower(challenge.scheme) {
	case "bearer":
		token, err := h.ociBearerToken(ctx, challenge)
		if err != nil {
			return nil, err
		}
		auth = "Bearer " + token
	case "basic":
		if basic := h.ociBasicAuthorization(); basic != "" {
			auth = basic
		}
	default:
		return nil, nil
	}
	if auth == "" {
		return nil, nil
	}
	request, err := http.NewRequestWithContext(ctx, method, targetURL, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("User-Agent", h.client.UserAgent)
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	request.Header.Set("Authorization", auth)
	return h.client.Do(request)
}

func (h *Handler) ociBearerToken(ctx context.Context, challenge ociChallenge) (string, error) {
	key := challenge.realm + "\x00" + challenge.params["service"] + "\x00" + challenge.params["scope"]
	now := time.Now()
	h.ociTokenMu.Lock()
	h.cleanupExpiredOCITokens(now)
	if cached := h.ociTokens[key]; cached.value != "" && now.Before(cached.expire) {
		h.ociTokenMu.Unlock()
		return cached.value, nil
	}
	h.ociTokenMu.Unlock()

	value, err, _ := h.ociGroup.Do(key, func() (any, error) {
		now := time.Now()
		h.ociTokenMu.Lock()
		h.cleanupExpiredOCITokens(now)
		if cached := h.ociTokens[key]; cached.value != "" && now.Before(cached.expire) {
			h.ociTokenMu.Unlock()
			return cached.value, nil
		}
		h.ociTokenMu.Unlock()

		token, expire, err := h.fetchOCIBearerToken(ctx, challenge, now)
		if err != nil {
			return "", err
		}
		h.ociTokenMu.Lock()
		h.ociTokens[key] = ociToken{value: token, expire: expire}
		h.ociTokenMu.Unlock()
		return token, nil
	})
	if err != nil {
		return "", err
	}
	return value.(string), nil
}

func (h *Handler) cleanupExpiredOCITokens(now time.Time) {
	for key, token := range h.ociTokens {
		if token.value == "" || !now.Before(token.expire) {
			delete(h.ociTokens, key)
		}
	}
}

func (h *Handler) fetchOCIBearerToken(ctx context.Context, challenge ociChallenge, now time.Time) (string, time.Time, error) {
	tokenURL, err := url.Parse(challenge.realm)
	if err != nil || tokenURL.Scheme == "" || tokenURL.Host == "" {
		return "", time.Time{}, fmt.Errorf("invalid OCI token realm %q", challenge.realm)
	}
	query := tokenURL.Query()
	for _, name := range []string{"service", "scope"} {
		if value := challenge.params[name]; value != "" {
			query.Set(name, value)
		}
	}
	tokenURL.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, tokenURL.String(), nil)
	if err != nil {
		return "", time.Time{}, err
	}
	request.Header.Set("User-Agent", h.client.UserAgent)
	if basic := h.ociBasicAuthorization(); basic != "" {
		request.Header.Set("Authorization", basic)
	}
	response, err := h.client.Do(request)
	if err != nil {
		return "", time.Time{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", time.Time{}, fmt.Errorf("OCI token request failed with %d", response.StatusCode)
	}
	var payload struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
		IssuedAt    string `json:"issued_at"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&payload); err != nil {
		return "", time.Time{}, err
	}
	token := payload.Token
	if token == "" {
		token = payload.AccessToken
	}
	if token == "" {
		return "", time.Time{}, errors.New("OCI token response is empty")
	}
	issuedAt := now
	if payload.IssuedAt != "" {
		if parsed, err := time.Parse(time.RFC3339, payload.IssuedAt); err == nil {
			issuedAt = parsed
		}
	}
	ttl := time.Duration(payload.ExpiresIn) * time.Second
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	expire := issuedAt.Add(ttl)
	if ttl > time.Minute {
		expire = expire.Add(-30 * time.Second)
	}
	return token, expire, nil
}

func parseOCIChallenge(header string) (ociChallenge, bool) {
	header = strings.TrimSpace(header)
	scheme, rest, ok := strings.Cut(header, " ")
	if !ok || scheme == "" {
		return ociChallenge{}, false
	}
	challenge := ociChallenge{scheme: scheme, params: map[string]string{}}
	for _, part := range splitChallengeParams(rest) {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		key = strings.ToLower(strings.TrimSpace(key))
		value = strings.Trim(strings.TrimSpace(value), `"`)
		value = strings.ReplaceAll(value, `\"`, `"`)
		if key == "realm" {
			challenge.realm = value
		}
		challenge.params[key] = value
	}
	if strings.EqualFold(challenge.scheme, "bearer") && challenge.realm == "" {
		return ociChallenge{}, false
	}
	return challenge, true
}

func splitChallengeParams(value string) []string {
	parts := []string{}
	start := 0
	quoted := false
	escaped := false
	for index, char := range value {
		if escaped {
			escaped = false
			continue
		}
		if char == '\\' {
			escaped = true
			continue
		}
		if char == '"' {
			quoted = !quoted
			continue
		}
		if char == ',' && !quoted {
			if part := strings.TrimSpace(value[start:index]); part != "" {
				parts = append(parts, part)
			}
			start = index + 1
		}
	}
	if part := strings.TrimSpace(value[start:]); part != "" {
		parts = append(parts, part)
	}
	return parts
}

func (h *Handler) staticAuthorization() string {
	if h.config.Mode == config.ModeOCI {
		if bearer := h.ociBearerAuthorization(); bearer != "" {
			return bearer
		}
		return ""
	}
	return ""
}

func (h *Handler) ociBearerAuthorization() string {
	if h.config.Mode != config.ModeOCI || h.config.OCIAuth == nil {
		return ""
	}
	auth := h.config.OCIAuth
	switch strings.ToLower(auth.Type) {
	case "bearer":
		if auth.Token == "" {
			return ""
		}
		return "Bearer " + auth.Token
	default:
		return ""
	}
}

func (h *Handler) ociBasicAuthorization() string {
	if h.config.Mode != config.ModeOCI || h.config.OCIAuth == nil || strings.ToLower(h.config.OCIAuth.Type) != "basic" {
		return ""
	}
	auth := h.config.OCIAuth
	if auth.Username == "" && auth.Password == "" {
		return ""
	}
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(auth.Username+":"+auth.Password))
}
