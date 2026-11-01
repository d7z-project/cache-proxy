package transport

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

// BindForward serves transparent read-only protocols without cache resources.
func BindForward(plan *proxyruntime.InstancePlan, mode string) error {
	client, err := NewClient(plan.Name(), mode, plan.Transport(), plan.UpstreamGate(), plan.Stats())
	if err != nil {
		return err
	}
	origin, err := url.Parse(plan.Upstream())
	if err != nil {
		return err
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if !proxyruntime.RequireReadMethod(w, req.Method) {
			return
		}
		cleaned, err := storeio.CleanURLPath(req.URL)
		if err != nil {
			http.Error(w, "invalid upstream path", http.StatusBadRequest)
			return
		}
		status, err := ForwardRead(req.Context(), client, origin, w, req, cleaned)
		if err != nil && status == 0 {
			status = http.StatusBadGateway
			WriteError(w, status)
		}
		plan.Stats().RecordRequest(plan.Name(), mode, req.Method, "BYPASS", status, 0)
	})
	return plan.BindPath(plan.Path(), proxyruntime.HandlerInstance{Handler: handler, CloseContext: func(context.Context) error {
		client.CloseIdleConnections()
		return nil
	}})
}

func ForwardRead(ctx context.Context, client *Client, origin *url.URL, writer http.ResponseWriter, inbound *http.Request, requestPath string) (int, error) {
	target, err := JoinURL(origin, EscapePathSegments(strings.TrimPrefix(requestPath, "/")), inbound.URL.RawQuery)
	if err != nil {
		return 0, err
	}
	return ForwardReadTarget(ctx, client, target, writer, inbound, inbound.Header)
}

// ForwardReadTarget proxies a read request to an already protocol-authorized target.
// Callers provide the exact safe upstream headers so local authorization
// tokens are not accidentally disclosed to a signed cross-origin URL.
func ForwardReadTarget(ctx context.Context, client *Client, target *url.URL, writer http.ResponseWriter, inbound *http.Request, header http.Header) (int, error) {
	if !proxyruntime.RequireReadMethod(writer, inbound.Method) {
		return http.StatusMethodNotAllowed, nil
	}
	request, err := http.NewRequestWithContext(ctx, inbound.Method, target.String(), nil)
	if err != nil {
		return 0, err
	}
	CopyReadRequestHeaders(request.Header, header)
	response, err := client.DoRead(ctx, request, AdmissionForeground)
	if err != nil {
		return 0, err
	}
	defer func() { _ = response.Body.Close() }()
	CopyEndToEndHeaders(writer.Header(), response.Header)
	writer.Header().Set("X-Cache", "BYPASS")
	writer.WriteHeader(response.StatusCode)
	if inbound.Method != http.MethodHead {
		if _, err := io.Copy(writer, response.Body); err != nil {
			return response.StatusCode, fmt.Errorf("copy upstream response: %w", err)
		}
	}
	return response.StatusCode, nil
}

func CopyReadRequestHeaders(destination, source http.Header) {
	CopyEndToEndHeaders(destination, source)
	SanitizeReadRequestHeaders(destination)
}

func SanitizeReadRequestHeaders(header http.Header) {
	for _, name := range []string{
		"Content-Encoding", "Content-Length", "Content-MD5", "Content-Type", "Digest", "Expect",
	} {
		header.Del(name)
	}
	SanitizeMethodOverrideHeaders(header)
}

func SanitizeMethodOverrideHeaders(header http.Header) {
	for _, name := range []string{"X-HTTP-Method", "X-HTTP-Method-Override", "X-Method-Override"} {
		header.Del(name)
	}
}
