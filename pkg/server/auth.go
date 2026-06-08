package server

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	sessionCookieName = "cache-proxy-session"
	sessionMaxAge     = 24 * time.Hour
	loginMaxAttempts  = 5
	loginWindow       = time.Minute
)

type sessionPayload struct {
	Exp int64 `json:"exp"`
}

type loginRateLimiter struct {
	mu       sync.Mutex
	attempts map[string][]time.Time
}

func newLoginRateLimiter() *loginRateLimiter {
	return &loginRateLimiter{attempts: map[string][]time.Time{}}
}

func (l *loginRateLimiter) allow(ip string) bool {
	now := time.Now()
	cutoff := now.Add(-loginWindow)
	l.mu.Lock()
	defer l.mu.Unlock()
	times := l.attempts[ip]
	recent := times[:0]
	for _, t := range times {
		if t.After(cutoff) {
			recent = append(recent, t)
		}
	}
	if len(recent) == 0 {
		l.attempts[ip] = []time.Time{now}
		return true
	}
	l.attempts[ip] = recent
	if len(recent) >= loginMaxAttempts {
		return false
	}
	l.attempts[ip] = append(recent, now)
	return true
}

func signSession(password string) (string, error) {
	payload := sessionPayload{Exp: time.Now().Add(sessionMaxAge).Unix()}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	enc := base64.RawURLEncoding.EncodeToString(data)
	mac := hmac.New(sha256.New, []byte(password))
	mac.Write([]byte(enc))
	sig := mac.Sum(nil)
	return enc + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

func verifySession(password, token string) bool {
	parts := strings.SplitN(token, ".", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return false
	}
	sigBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, []byte(password))
	mac.Write([]byte(parts[0]))
	expected := mac.Sum(nil)
	if !hmac.Equal(sigBytes, expected) {
		return false
	}
	data, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return false
	}
	var payload sessionPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return false
	}
	return time.Now().Unix() < payload.Exp
}

func setSessionCookie(w http.ResponseWriter, token string) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		MaxAge:   int(sessionMaxAge.Seconds()),
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	})
}

func clearSessionCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	})
}

func extractSession(req *http.Request) string {
	cookie, err := req.Cookie(sessionCookieName)
	if err != nil || cookie.Value == "" {
		return ""
	}
	return cookie.Value
}

func adminAuthMiddleware(password string, next http.Handler) http.HandlerFunc {
	if password == "" {
		return func(w http.ResponseWriter, r *http.Request) { next.ServeHTTP(w, r) }
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if verifySession(password, extractSession(r)) {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized"})
	}
}

func metricsAuthMiddleware(token string, next http.Handler) http.HandlerFunc {
	if token == "" {
		return func(w http.ResponseWriter, r *http.Request) { next.ServeHTTP(w, r) }
	}
	expected := computeTokenHash(token)
	return func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		actual := computeTokenHash(strings.TrimPrefix(auth, "Bearer "))
		if !strings.HasPrefix(auth, "Bearer ") || !hmac.Equal(expected, actual) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	}
}

func computeTokenHash(value string) []byte {
	mac := hmac.New(sha256.New, nil)
	mac.Write([]byte(value))
	return mac.Sum(nil)
}

func (r *Runtime) loginHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if r.password == "" {
		writeJSON(w, map[string]bool{"ok": true}, nil)
		return
	}
	body, err := io.ReadAll(io.LimitReader(req.Body, 4096))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var input struct {
		Password string `json:"password"`
	}
	if err := json.Unmarshal(body, &input); err != nil {
		writeError(w, http.StatusBadRequest, errors.New("invalid request body"))
		return
	}

	ip, _, _ := net.SplitHostPort(req.RemoteAddr)
	if ip == "" {
		ip = req.RemoteAddr
	}
	if !r.loginLimiter.allow(ip) {
		writeError(w, http.StatusTooManyRequests, errors.New("too many login attempts"))
		return
	}
	if input.Password != r.password {
		writeError(w, http.StatusUnauthorized, errors.New("invalid password"))
		return
	}
	token, err := signSession(r.password)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	setSessionCookie(w, token)
	writeJSON(w, map[string]bool{"ok": true}, nil)
}

func (r *Runtime) logoutHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	clearSessionCookie(w)
	writeJSON(w, map[string]bool{"ok": true}, nil)
}
