package git

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/go-git/go-git/v5/plumbing/format/pktline"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/plumbing/transport"

	proxytransport "gopkg.d7z.net/cache-proxy/pkg/proxy/internal/transport"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

type singleLoader struct {
	storer storer.Storer
}

func (l *singleLoader) Load(_ *transport.Endpoint) (storer.Storer, error) {
	return l.storer, nil
}

func serveGitHTTP(w http.ResponseWriter, r *http.Request, svr transport.Transport, name string) {
	switch {
	case r.URL.Path == "/info/refs" && r.URL.Query().Get("service") == "git-upload-pack":
		handleInfoRefs(w, r, svr, name)
	case r.URL.Path == "/git-upload-pack":
		handleUploadPack(w, r, svr, name)
	default:
		http.NotFound(w, r)
	}
}

func (h *gitHandler) forwardUpstream(w http.ResponseWriter, request *http.Request, cleaned string) {
	origin, err := url.Parse(h.upstream)
	if err != nil {
		proxytransport.WriteError(w, http.StatusBadGateway)
		return
	}
	upstreamURL, err := proxytransport.JoinURL(origin, proxytransport.EscapePathSegments(cleaned), request.URL.RawQuery)
	if err != nil {
		proxytransport.WriteError(w, http.StatusBadGateway)
		return
	}
	var body io.Reader
	if request.Method == http.MethodPost {
		body = request.Body
	}
	proxyRequest, err := http.NewRequestWithContext(request.Context(), request.Method, upstreamURL.String(), body)
	if err != nil {
		proxytransport.WriteError(w, http.StatusBadGateway)
		return
	}
	proxytransport.CopyEndToEndHeaders(proxyRequest.Header, request.Header)
	if request.Method == http.MethodPost {
		proxytransport.SanitizeMethodOverrideHeaders(proxyRequest.Header)
	} else {
		proxytransport.SanitizeReadRequestHeaders(proxyRequest.Header)
	}
	if auth, ok := h.auth.(interface{ SetAuth(*http.Request) }); ok {
		auth.SetAuth(proxyRequest)
	}
	proxyRequest = proxytransport.WithAdmission(request.Context(), proxyRequest, proxytransport.AdmissionForeground)
	response, err := h.bootstrapClient.Do(proxyRequest)
	if err != nil {
		if _, ok := proxyruntime.AdmissionRetryAfterSeconds(err); ok {
			h.writeAdmissionError(w, err)
			return
		}
		if !errors.Is(err, request.Context().Err()) {
			slog.Warn("git bootstrap proxy failed", "instance", h.name, "upstream", h.redactedUpstream(), "err", err)
		}
		proxytransport.WriteError(w, http.StatusBadGateway)
		return
	}
	defer func() { _ = response.Body.Close() }()
	proxytransport.CopyEndToEndHeaders(w.Header(), response.Header)
	w.WriteHeader(response.StatusCode)
	if request.Method != http.MethodHead {
		_, _ = io.Copy(w, response.Body)
	}
}

func isGitReadRequest(request *http.Request) bool {
	if _, err := storeio.CleanURLPath(request.URL); err != nil {
		return false
	}
	services, serviceSet := request.URL.Query()["service"]
	return request.Method == http.MethodGet && request.URL.Path == "/info/refs" && len(services) == 1 && services[0] == "git-upload-pack" ||
		request.Method == http.MethodPost && request.URL.Path == "/git-upload-pack" ||
		(request.Method == http.MethodGet || request.Method == http.MethodHead) && (!serviceSet || len(services) == 1 && services[0] == "") && isDumbGitPath(request.URL.Path)
}

func shouldForwardGitRead(request *http.Request) bool {
	return strings.Contains(request.Header.Get("Git-Protocol"), "version=2") ||
		(request.Method == http.MethodGet || request.Method == http.MethodHead) && request.URL.Query().Get("service") == "" && isDumbGitPath(request.URL.Path)
}

func isDumbGitPath(requestPath string) bool {
	return requestPath == "/HEAD" || requestPath == "/info/refs" || requestPath == "/objects/info/packs" ||
		requestPath == "/objects/info/alternates" || requestPath == "/objects/info/http-alternates" ||
		strings.HasPrefix(requestPath, "/objects/pack/") || strings.HasPrefix(requestPath, "/objects/")
}

func handleInfoRefs(w http.ResponseWriter, r *http.Request, svr transport.Transport, name string) {
	ep, _ := transport.NewEndpoint("file://")
	session, err := svr.NewUploadPackSession(ep, nil)
	if err != nil {
		slog.Error("git info/refs session failed", "instance", name, "err", err)
		proxytransport.WriteError(w, http.StatusInternalServerError)
		return
	}
	defer func() { _ = session.Close() }() // Close error is non-actionable after session use

	ar, err := session.AdvertisedReferencesContext(r.Context())
	if err != nil {
		slog.Error("git info/refs advertised refs failed", "instance", name, "err", err)
		proxytransport.WriteError(w, http.StatusInternalServerError)
		return
	}
	ar.Prefix = [][]byte{[]byte("# service=git-upload-pack"), pktline.Flush}

	w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	if err := ar.Encode(w); err != nil {
		slog.Error("git info/refs encode failed", "instance", name, "err", err)
	}
}

func handleUploadPack(w http.ResponseWriter, r *http.Request, svr transport.Transport, name string) {
	ep, _ := transport.NewEndpoint("file://")
	session, err := svr.NewUploadPackSession(ep, nil)
	if err != nil {
		slog.Error("git upload-pack session failed", "instance", name, "err", err)
		proxytransport.WriteError(w, http.StatusInternalServerError)
		return
	}
	defer func() { _ = session.Close() }() // Close error is non-actionable after session use

	req := packp.NewUploadPackRequest()
	if err := req.Decode(r.Body); err != nil {
		slog.Error("git upload-pack decode failed", "instance", name, "err", err)
		proxytransport.WriteError(w, http.StatusInternalServerError)
		return
	}

	resp, err := session.UploadPack(r.Context(), req)
	if err != nil {
		slog.Error("git upload-pack failed", "instance", name, "err", err)
		proxytransport.WriteError(w, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	if err := resp.Encode(w); err != nil {
		slog.Error("git upload-pack encode failed", "instance", name, "err", err)
	}
}

func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	u.User = nil
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}
