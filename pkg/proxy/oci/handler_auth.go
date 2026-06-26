package oci

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/utils"
)

const maxTokenResponseSize = 1 << 20 // 1MB

func (h *handler) retryChallenge(ctx context.Context, method, targetURL string, headers map[string]string, response *http.Response) (*http.Response, error) {
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
		auth = h.basicAuthorization()
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

func (h *handler) staticAuthorization() string {
	if h.policy.Auth == nil || strings.ToLower(h.policy.Auth.Type) != "bearer" || h.policy.Auth.Token == "" {
		return ""
	}
	return "Bearer " + h.policy.Auth.Token
}

func (h *handler) basicAuthorization() string {
	if h.policy.Auth == nil || strings.ToLower(h.policy.Auth.Type) != "basic" {
		return ""
	}
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(h.policy.Auth.Username+":"+h.policy.Auth.Password))
}

func (h *handler) ociBearerToken(ctx context.Context, challenge ociChallenge) (string, error) {
	key := challenge.realm + "\x00" + challenge.params["service"] + "\x00" + challenge.params["scope"]
	now := time.Now()
	h.auth.tokenMu.Lock()
	for itemKey, token := range h.auth.tokens {
		if token.value == "" || !now.Before(token.expire) {
			delete(h.auth.tokens, itemKey)
		}
	}
	if token := h.auth.tokens[key]; token.value != "" && now.Before(token.expire) {
		h.auth.tokenMu.Unlock()
		return token.value, nil
	}
	h.auth.tokenMu.Unlock()

	value, err, _ := h.auth.group.Do(key, func() (any, error) {
		token, expire, err := h.fetchBearerToken(ctx, challenge, time.Now())
		if err != nil {
			return "", err
		}
		h.auth.tokenMu.Lock()
		h.auth.tokens[key] = ociToken{value: token, expire: expire}
		h.auth.tokenMu.Unlock()
		return token, nil
	})
	if err != nil {
		return "", err
	}
	token, ok := value.(string)
	if !ok {
		return "", errors.New("token type assertion failed")
	}
	return token, nil
}

func (h *handler) fetchBearerToken(ctx context.Context, challenge ociChallenge, now time.Time) (string, time.Time, error) {
	tokenURL, err := url.Parse(challenge.realm)
	if err != nil || tokenURL.Scheme == "" || tokenURL.Host == "" {
		return "", time.Time{}, errors.New("invalid OCI token realm")
	}
	query := tokenURL.Query()
	for _, key := range []string{"service", "scope"} {
		if value := challenge.params[key]; value != "" {
			query.Set(key, value)
		}
	}
	tokenURL.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, tokenURL.String(), nil)
	if err != nil {
		return "", time.Time{}, err
	}
	request.Header.Set("User-Agent", h.client.UserAgent)
	if basic := h.basicAuthorization(); basic != "" {
		request.Header.Set("Authorization", basic)
	}
	response, err := h.client.Do(request)
	if err != nil {
		return "", time.Time{}, err
	}
	defer response.Body.Close()
	response.Body = utils.NewRateLimitReader(response.Body)
	if response.StatusCode != http.StatusOK {
		return "", time.Time{}, errors.New("OCI token request failed")
	}
	var payload struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
		IssuedAt    string `json:"issued_at"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, maxTokenResponseSize)).Decode(&payload); err != nil {
		return "", time.Time{}, err
	}
	token := payload.Token
	if token == "" {
		token = payload.AccessToken
	}
	if token == "" {
		return "", time.Time{}, errors.New("OCI token response is empty")
	}
	if payload.IssuedAt != "" {
		if issuedAt, err := time.Parse(time.RFC3339, payload.IssuedAt); err == nil {
			now = issuedAt
		}
	}
	ttl := time.Duration(payload.ExpiresIn) * time.Second
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	expire := now.Add(ttl)
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
	var parts []string
	start := 0
	quoted := false
	escaped := false
	for index, char := range value {
		if escaped {
			escaped = false
			continue
		}
		switch char {
		case '\\':
			escaped = true
		case '"':
			quoted = !quoted
		case ',':
			if !quoted {
				if part := strings.TrimSpace(value[start:index]); part != "" {
					parts = append(parts, part)
				}
				start = index + 1
			}
		}
	}
	if part := strings.TrimSpace(value[start:]); part != "" {
		parts = append(parts, part)
	}
	return parts
}
