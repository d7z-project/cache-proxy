package git

import (
	"log/slog"
	"net/http"
	"net/url"

	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/plumbing/transport"
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

func handleInfoRefs(w http.ResponseWriter, r *http.Request, svr transport.Transport, name string) {
	ep, _ := transport.NewEndpoint("file://")
	session, err := svr.NewUploadPackSession(ep, nil)
	if err != nil {
		slog.Error("git info/refs session failed", "instance", name, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	defer func() { _ = session.Close() }() // Close error is non-actionable after session use

	ar, err := session.AdvertisedReferencesContext(r.Context())
	if err != nil {
		slog.Error("git info/refs advertised refs failed", "instance", name, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	ar.Prefix = [][]byte{[]byte("# service=git-upload-pack\n")}

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
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	defer func() { _ = session.Close() }() // Close error is non-actionable after session use

	req := packp.NewUploadPackRequest()
	if err := req.Decode(r.Body); err != nil {
		slog.Error("git upload-pack decode failed", "instance", name, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	resp, err := session.UploadPack(r.Context(), req)
	if err != nil {
		slog.Error("git upload-pack failed", "instance", name, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
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
