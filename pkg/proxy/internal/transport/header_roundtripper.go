package transport

import (
	"errors"
	"net/http"
)

type headerBoundaryRoundTripper struct {
	base      http.RoundTripper
	userAgent string
}

// ConfigureHeaderBoundary applies the request and response policy to every
// transport hop. It must run after transport options and admission are set.
func ConfigureHeaderBoundary(client *http.Client, userAgent string) {
	if client == nil {
		return
	}
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	client.Transport = &headerBoundaryRoundTripper{base: base, userAgent: userAgent}
}

func (t *headerBoundaryRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if request == nil || request.URL == nil {
		return nil, errors.New("upstream request URL is missing")
	}
	if request.URL.User != nil {
		return nil, errors.New("upstream request URL must not contain user information")
	}
	outbound := request.Clone(request.Context())
	outbound.Header = request.Header.Clone()
	SanitizeUpstreamRequest(outbound, t.userAgent)
	response, err := t.base.RoundTrip(outbound)
	if err != nil {
		return nil, err
	}
	SanitizeUpstreamResponse(response)
	return response, nil
}

func (t *headerBoundaryRoundTripper) CloseIdleConnections() {
	if closer, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}
