package transport

import (
	"context"
	"io"
	"net/http"
	"sync"
)

type admissionRequest struct {
	ctx   context.Context
	class AdmissionClass
}

type admissionContextKey struct{}

type admissionRoundTripper struct {
	base http.RoundTripper
	gate *UpstreamGate
}

type admissionBody struct {
	io.ReadCloser
	release func()
	once    sync.Once
}

// ConfigureAdmission applies host admission to each transport request,
// including every hop of a redirect chain.
func ConfigureAdmission(client *http.Client, gate *UpstreamGate) {
	if client == nil || gate == nil {
		return
	}
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	client.Transport = &admissionRoundTripper{base: base, gate: gate}
}

// WithAdmission associates the admission wait context and request class with
// an HTTP request. The request's existing context remains the transfer context.
func WithAdmission(admissionCtx context.Context, request *http.Request, class AdmissionClass) *http.Request {
	if request == nil {
		return nil
	}
	if admissionCtx == nil {
		admissionCtx = request.Context()
	}
	ctx := context.WithValue(request.Context(), admissionContextKey{}, admissionRequest{ctx: admissionCtx, class: class})
	return request.WithContext(ctx)
}

func (t *admissionRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	admission, ok := request.Context().Value(admissionContextKey{}).(admissionRequest)
	if !ok {
		admission = admissionRequest{ctx: request.Context(), class: AdmissionForeground}
	}
	release, err := t.gate.Acquire(admission.ctx, request.URL.String(), admission.class)
	if err != nil {
		return nil, err
	}
	response, err := t.base.RoundTrip(request)
	if err != nil {
		release()
		return nil, err
	}
	if response.StatusCode == http.StatusTooManyRequests {
		_ = t.gate.RateLimited(request.URL.String(), response.Header.Get("Retry-After"))
	}
	response.Body = &admissionBody{ReadCloser: response.Body, release: release}
	return response, nil
}

func (b *admissionBody) Read(buffer []byte) (int, error) {
	n, err := b.ReadCloser.Read(buffer)
	if err != nil {
		b.finish()
	}
	return n, err
}

func (b *admissionBody) Close() error {
	err := b.ReadCloser.Close()
	b.finish()
	return err
}

func (b *admissionBody) finish() {
	b.once.Do(b.release)
}
