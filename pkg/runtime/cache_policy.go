package runtime

import (
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// CachePolicy is HTTP reuse policy, independent of protocol cache identity.
type CachePolicy struct {
	NoCache, NoStore, Private, MustRevalidate bool
	Lifetime                                  time.Duration
	Explicit                                  bool
}

// ParseCachePolicy bounds both work and arithmetic on untrusted header values.
func ParseCachePolicy(header http.Header, received time.Time, fallback time.Duration) CachePolicy {
	p := CachePolicy{Lifetime: fallback}
	var maxAge, sharedAge time.Duration
	var hasMax, hasShared, invalid bool
	bytes, count := 0, 0
	for _, line := range header.Values("Cache-Control") {
		bytes += len(line)
		if bytes > 64<<10 {
			p.NoCache, p.NoStore = true, true
			return p
		}
		for field := range strings.SplitSeq(line, ",") {
			count++
			if count > 256 {
				p.NoCache, p.NoStore = true, true
				return p
			}
			name, value, _ := strings.Cut(strings.TrimSpace(field), "=")
			switch strings.ToLower(strings.TrimSpace(name)) {
			case "no-cache":
				p.NoCache = true
			case "no-store":
				p.NoStore = true
			case "private":
				p.Private = true
			case "must-revalidate", "proxy-revalidate":
				p.MustRevalidate = true
			case "max-age", "s-maxage":
				value = strings.Trim(strings.TrimSpace(value), `"`)
				seconds, err := strconv.ParseUint(value, 10, 64)
				if err != nil || seconds > uint64(math.MaxInt64/int64(time.Second)) {
					invalid = true
					continue
				}
				age := time.Duration(seconds) * time.Second
				if strings.EqualFold(strings.TrimSpace(name), "s-maxage") {
					invalid = invalid || hasShared
					hasShared, sharedAge, p.MustRevalidate = true, age, true
				} else {
					invalid = invalid || hasMax
					hasMax, maxAge = true, age
				}
			}
		}
	}
	switch {
	case hasShared:
		p.Lifetime, p.Explicit = sharedAge, true
	case hasMax:
		p.Lifetime, p.Explicit = maxAge, true
	case header.Get("Expires") != "":
		p.Explicit = true
		date, err := http.ParseTime(header.Get("Date"))
		if err != nil {
			date = received
		}
		expires, err := http.ParseTime(header.Get("Expires"))
		if err != nil {
			invalid = true
		} else {
			p.Lifetime = max(0, expires.Sub(date))
		}
	}
	if fallback > 0 && p.Lifetime > fallback {
		p.Lifetime = fallback
	}
	p.NoCache = p.NoCache || invalid
	return p
}

func RequestForcesRevalidation(request *http.Request) bool {
	p := ParseCachePolicy(request.Header, time.Now(), 0)
	return p.NoCache || p.Explicit && p.Lifetime == 0 ||
		request.Header.Get("Cache-Control") == "" && strings.EqualFold(strings.TrimSpace(request.Header.Get("Pragma")), "no-cache")
}

func ResponseFresh(header http.Header, fetched time.Time, fallback time.Duration) bool {
	p := ParseCachePolicy(header, fetched, fallback)
	return !fetched.IsZero() && !p.NoCache && !p.NoStore && ResponseAge(header, fetched, time.Now()) < p.Lifetime
}

func ResponseAge(header http.Header, received, now time.Time) time.Duration {
	age := time.Duration(0)
	if value := header.Get("Age"); value != "" {
		seconds, err := strconv.ParseUint(value, 10, 64)
		if err != nil || seconds > uint64(math.MaxInt64/int64(time.Second)) {
			return time.Duration(math.MaxInt64)
		}
		age = time.Duration(seconds) * time.Second
	}
	if date, err := http.ParseTime(header.Get("Date")); err == nil {
		age = max(age, received.Sub(date))
	}
	resident := max(time.Duration(0), now.Sub(received))
	if age > time.Duration(math.MaxInt64)-resident {
		return time.Duration(math.MaxInt64)
	}
	return age + resident
}

func StaleAllowed(request *http.Request, header http.Header) bool {
	p := ParseCachePolicy(header, time.Now(), 0)
	return !RequestForcesRevalidation(request) && !p.NoCache && !p.NoStore && !p.MustRevalidate
}

// MergeRevalidationHeader retains representation metadata absent from a 304,
// while replacing its age rather than carrying the previous response's age.
func MergeRevalidationHeader(stored, update http.Header) http.Header {
	header := stored.Clone()
	if header == nil {
		header = make(http.Header)
	}
	header.Del("Date")
	header.Del("Age")
	for _, name := range []string{"Cache-Control", "Content-Location", "ETag", "Expires", "Last-Modified", "Vary", "Date", "Age", "X-Source-ETag", "X-Source-Last-Modified"} {
		if values := update.Values(name); len(values) > 0 {
			header[http.CanonicalHeaderKey(name)] = append([]string(nil), values...)
		}
	}
	return header
}
