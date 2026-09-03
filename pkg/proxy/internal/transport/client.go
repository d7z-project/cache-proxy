package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/metrics"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

const maxResponseHeaderBytes = 1 << 20

type Client struct {
	instance            string
	mode                string
	httpClient          *http.Client
	stats               *metrics.Stats
	userAgent           string
	userAgentConfigured bool
	idleBodyTimeout     time.Duration
	spoolMu             sync.Mutex
	spooler             *storeio.Spooler
}

func (c *Client) SetSpooler(spooler *storeio.Spooler) {
	c.spoolMu.Lock()
	c.spooler = spooler
	c.spoolMu.Unlock()
}

func (c *Client) Spooler() *storeio.Spooler {
	c.spoolMu.Lock()
	defer c.spoolMu.Unlock()
	return c.spooler
}

func (c *Client) EnsureSpooler(workDir string) *storeio.Spooler {
	c.spoolMu.Lock()
	defer c.spoolMu.Unlock()
	if c.spooler == nil {
		c.spooler = storeio.NewSpooler(workDir, 2<<30, nil)
	}
	return c.spooler
}

func NewClient(instance, mode string, cfg *config.TransportConfig, gate *UpstreamGate, stats *metrics.Stats) (*Client, error) {
	if err := config.ValidateTransport(cfg); err != nil {
		return nil, fmt.Errorf("configure transport: %w", err)
	}
	baseClient := NewUpstreamHTTPClient()
	httpTransport, ok := baseClient.Transport.(*http.Transport)
	if !ok {
		return nil, errors.New("default HTTP transport has unexpected type")
	}
	// Protocol digests are defined over the upstream wire representation. Keep
	// content codings intact instead of allowing net/http to transparently
	// decompress responses before a mode verifies or stores them.
	httpTransport.DisableCompression = true
	httpTransport.MaxResponseHeaderBytes = maxResponseHeaderBytes
	if cfg != nil {
		if cfg.Proxy != "" {
			proxyURL, _ := url.Parse(strings.TrimSpace(cfg.Proxy))
			httpTransport.Proxy = http.ProxyURL(proxyURL)
		}
		if cfg.DialTimeout > 0 {
			httpTransport.DialContext = (&net.Dialer{Timeout: cfg.DialTimeout.Duration(), KeepAlive: 30 * time.Second}).DialContext
		}
		if cfg.HeaderTimeout > 0 {
			httpTransport.ResponseHeaderTimeout = cfg.HeaderTimeout.Duration()
		}
		if cfg.MaxIdleConns > 0 {
			httpTransport.MaxIdleConns = cfg.MaxIdleConns
		}
	}
	client := &http.Client{
		Transport:     httpTransport,
		Timeout:       baseClient.Timeout,
		CheckRedirect: CheckReadOnlyRedirect,
	}
	ConfigureAdmission(client, gate)
	if cfg != nil && cfg.MaxRequestDuration > 0 {
		client.Timeout = cfg.MaxRequestDuration.Duration()
	}
	result := &Client{
		instance: instance, mode: mode, httpClient: client, stats: stats,
		userAgent: DefaultUserAgent, idleBodyTimeout: baseClient.IdleBodyTimeout,
	}
	if cfg != nil {
		if cfg.UserAgent != "" {
			result.userAgent = cfg.UserAgent
			result.userAgentConfigured = true
		}
		if cfg.IdleBodyTimeout > 0 {
			result.idleBodyTimeout = cfg.IdleBodyTimeout.Duration()
		}
	}
	return result, nil
}

// NewPlanClient builds an upstream client from an instance plan and attaches
// its shared spool budget.
func NewPlanClient(plan *proxyruntime.InstancePlan, mode string) (*Client, error) {
	client, err := NewClient(plan.Name(), mode, plan.Transport(), plan.UpstreamGate(), plan.Stats())
	if err != nil {
		return nil, err
	}
	client.SetSpooler(storeio.NewSpooler(filepath.Join(plan.StoreRoot(), "work"), plan.MaxCacheObjectSize(), plan.SpoolBudget()))
	return client, nil
}

func ConfigureHTTPClient(client *UpstreamHTTPClient, instance string, cfg *config.TransportConfig) {
	client.UserAgent = DefaultUserAgent
	client.UserAgentConfigured = false
	base, ok := client.Transport.(*http.Transport)
	if !ok {
		slog.Warn("cannot configure transport", "instance", instance)
		return
	}
	base.DisableCompression = true
	base.MaxResponseHeaderBytes = maxResponseHeaderBytes
	client.CheckRedirect = CheckReadOnlyRedirect
	if cfg == nil {
		return
	}
	if cfg.UserAgent != "" {
		client.UserAgent = cfg.UserAgent
		client.UserAgentConfigured = true
	}
	if cfg.Proxy != "" {
		if proxyURL, err := url.Parse(strings.TrimSpace(cfg.Proxy)); err == nil {
			base.Proxy = http.ProxyURL(proxyURL)
		}
	}
	if cfg.DialTimeout > 0 {
		base.DialContext = (&net.Dialer{Timeout: cfg.DialTimeout.Duration(), KeepAlive: 30 * time.Second}).DialContext
	}
	if cfg.HeaderTimeout > 0 {
		base.ResponseHeaderTimeout = cfg.HeaderTimeout.Duration()
	}
	if cfg.MaxIdleConns > 0 {
		base.MaxIdleConns = cfg.MaxIdleConns
	}
	if cfg.MaxRequestDuration > 0 {
		client.Timeout = cfg.MaxRequestDuration.Duration()
	}
	if cfg.IdleBodyTimeout > 0 {
		client.IdleBodyTimeout = cfg.IdleBodyTimeout.Duration()
	}
}

func (c *Client) DoRead(ctx context.Context, request *http.Request, class AdmissionClass) (*http.Response, error) {
	if request != nil && (request.Method != http.MethodGet && request.Method != http.MethodHead || request.Body != nil && request.Body != http.NoBody) {
		return nil, errors.New("upstream read request must be bodyless GET or HEAD")
	}
	return c.send(ctx, request, class)
}

func (c *Client) DoReadOnlyPost(ctx context.Context, request *http.Request, class AdmissionClass) (*http.Response, error) {
	if request == nil || request.Method != http.MethodPost {
		return nil, errors.New("upstream read-only query must use POST")
	}
	return c.send(ctx, request, class)
}

func (c *Client) send(ctx context.Context, request *http.Request, class AdmissionClass) (*http.Response, error) {
	if request == nil || request.URL == nil || request.URL.Scheme == "" || request.URL.Host == "" {
		return nil, errors.New("invalid upstream request")
	}
	releaseStats := c.stats.BeginUpstreamRequest(c.instance, c.mode, RedactedURL(request.URL))
	if c.userAgentConfigured || request.Header.Get("User-Agent") == "" {
		request.Header.Set("User-Agent", c.userAgent)
	}
	started := time.Now()
	request = WithAdmission(ctx, request.WithContext(ctx), class)
	response, err := c.httpClient.Do(request)
	if err != nil {
		releaseStats()
		c.stats.RecordUpstreamRequest(c.instance, c.mode, RedactedURL(request.URL), request.Method, 0, time.Since(started), 0)
		return nil, fmt.Errorf("upstream request: %w", err)
	}
	responseURL := request.URL
	if response.Request != nil && response.Request.URL != nil {
		responseURL = response.Request.URL
	}
	response.Body = &observedBody{
		ReadCloser: newIdleTimeoutReadCloser(response.Body, c.idleBodyTimeout),
		done: func(bytes uint64) {
			releaseStats()
			c.stats.RecordUpstreamRequest(c.instance, c.mode, RedactedURL(responseURL), request.Method, response.StatusCode, time.Since(started), bytes)
		},
	}
	return response, nil
}

func (c *Client) RequestUserAgent(request *http.Request) (string, bool) {
	if c.userAgentConfigured || !IsBrowserRequest(request) {
		return c.userAgent, false
	}
	return request.UserAgent(), true
}

func (c *Client) UserAgentConfigured() bool { return c.userAgentConfigured }

func SameOrigin(left, right *url.URL) bool {
	if left == nil || right == nil || !strings.EqualFold(left.Scheme, right.Scheme) {
		return false
	}
	return strings.EqualFold(left.Hostname(), right.Hostname()) && effectivePort(left) == effectivePort(right)
}

// CheckReadOnlyRedirect permits ordinary read redirects while preventing a
// protocol-authorized POST body from being redirected to another operation.
func CheckReadOnlyRedirect(request *http.Request, via []*http.Request) error {
	if len(via) == 0 {
		return nil
	}
	previous := via[len(via)-1]
	if via[0].Method != http.MethodGet && via[0].Method != http.MethodHead {
		if request.Method != via[0].Method || !SameOrigin(request.URL, previous.URL) || request.URL.EscapedPath() != via[0].URL.EscapedPath() {
			return errors.New("non-read redirect changed upstream operation")
		}
	}
	if !SameOrigin(request.URL, previous.URL) {
		request.Header.Del("Authorization")
		request.Header.Del("Cookie")
	}
	return nil
}

func effectivePort(value *url.URL) string {
	if port := value.Port(); port != "" {
		return port
	}
	if strings.EqualFold(value.Scheme, "https") {
		return "443"
	}
	if strings.EqualFold(value.Scheme, "http") {
		return "80"
	}
	return ""
}

func (c *Client) CloseIdleConnections() {
	c.httpClient.CloseIdleConnections()
}

type observedBody struct {
	io.ReadCloser
	bytes uint64
	done  func(uint64)
	once  sync.Once
}

func (b *observedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.bytes += uint64(n)
	if err != nil {
		b.finish()
	}
	return n, err
}

func (b *observedBody) Close() error {
	err := b.ReadCloser.Close()
	b.finish()
	return err
}

func (b *observedBody) finish() {
	b.once.Do(func() { b.done(b.bytes) })
}

func JoinURL(base *url.URL, escapedPath, rawQuery string) (*url.URL, error) {
	if base == nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return nil, errors.New("invalid upstream base URL")
	}
	result := *base
	basePath := strings.TrimSuffix(base.EscapedPath(), "/")
	result.RawPath = basePath + "/" + strings.TrimPrefix(escapedPath, "/")
	decoded, err := url.PathUnescape(result.RawPath)
	if err != nil {
		return nil, fmt.Errorf("decode upstream path: %w", err)
	}
	result.Path = decoded
	result.RawQuery = rawQuery
	result.Fragment = ""
	return &result, nil
}

func RedactedURL(value *url.URL) string {
	if value == nil {
		return ""
	}
	redacted := *value
	redacted.User = nil
	redacted.RawQuery = ""
	redacted.Fragment = ""
	return redacted.String()
}

func CopyEndToEndHeaders(destination, source http.Header) {
	proxyruntime.CopyEndToEndHeaders(destination, source)
}
