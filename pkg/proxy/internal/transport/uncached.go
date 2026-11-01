package transport

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

// ForwardTransformed creates a request-local representation. Its validators
// belong to the rewritten body, never to the upstream representation.
func ForwardTransformed(ctx context.Context, client *Client, origin *url.URL, w http.ResponseWriter, req *http.Request, requestPath string, spooler *storeio.Spooler, limit int64, rewrite func(io.Reader, io.Writer, http.Header) error) (int, error) {
	target, err := JoinURL(origin, EscapePathSegments(requestPath), req.URL.RawQuery)
	if err != nil {
		return 0, err
	}
	upstream, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return 0, err
	}
	CopyReadRequestHeaders(upstream.Header, req.Header)
	for _, key := range []string{"If-Match", "If-None-Match", "If-Modified-Since", "If-Unmodified-Since", "Range", "If-Range"} {
		upstream.Header.Del(key)
	}
	upstream.Header.Set("Accept-Encoding", "identity")
	response, err := client.DoRead(ctx, upstream, AdmissionForeground)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Encoding") != "" && response.Header.Get("Content-Encoding") != "identity" {
		return WriteResponse(w, req, response, "BYPASS"), nil
	}
	source, err := spooler.SpoolWithExpectedSize(ctx, response.Body, limit, response.ContentLength)
	if err != nil {
		return 0, err
	}
	defer source.Close()
	_ = response.Body.Close()
	reader, writer := io.Pipe()
	var rewriteGroup sync.WaitGroup
	var rewriteErr error
	rewriteGroup.Go(func() {
		rewriteErr = rewrite(source.File, writer, response.Header)
		_ = writer.CloseWithError(rewriteErr)
	})
	output, err := spooler.SpoolWithExpectedSize(ctx, reader, limit*4, -1)
	_ = reader.CloseWithError(err)
	rewriteGroup.Wait()
	if err != nil {
		return 0, err
	}
	defer output.Close()
	if rewriteErr != nil {
		return 0, rewriteErr
	}
	CopyEndToEndHeaders(w.Header(), response.Header)
	for _, key := range []string{"Content-Length", "Content-Encoding", "Content-MD5", "Digest", "Content-Digest", "Repr-Digest", "Last-Modified", "ETag"} {
		w.Header().Del(key)
	}
	w.Header().Set("ETag", `"sha256-`+output.SHA256+`"`)
	// Rewritten links can contain expiring, credential-scoped authorizations.
	// Revalidate the representation before reuse even when the source is public.
	if proxyruntime.ParseCachePolicy(response.Header, time.Now(), 0).NoStore {
		w.Header().Set("Cache-Control", "private, no-store")
	} else {
		w.Header().Set("Cache-Control", "private, no-cache")
	}
	w.Header().Set("X-Cache", "BYPASS")
	recorder := &contentStatusWriter{ResponseWriter: w, status: http.StatusOK}
	http.ServeContent(recorder, req, requestPath, time.Time{}, output.File)
	return recorder.status, nil
}

// ForwardVerified releases its temporary body after this request; it never
// publishes it or shares it with another download.
func ForwardVerified(ctx context.Context, client *Client, target *url.URL, w http.ResponseWriter, req *http.Request, header http.Header, spooler *storeio.Spooler, verify func(io.ReadSeeker) error) (int, error) {
	if req.Method == http.MethodHead || req.Header.Get("Range") != "" || verify == nil {
		return ForwardReadTarget(ctx, client, target, w, req, header)
	}
	upstream, err := http.NewRequestWithContext(ctx, req.Method, target.String(), nil)
	if err != nil {
		return 0, err
	}
	CopyReadRequestHeaders(upstream.Header, header)
	upstream.Header.Set("Accept-Encoding", "identity")
	response, err := client.DoRead(ctx, upstream, AdmissionForeground)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return WriteResponse(w, req, response, "BYPASS"), nil
	}
	spool, err := spooler.SpoolWithExpectedSize(ctx, response.Body, 0, response.ContentLength)
	if err != nil {
		return 0, err
	}
	defer spool.Close()
	_ = response.Body.Close()
	if err := verify(spool.File); err != nil {
		return 0, err
	}
	if _, err := spool.File.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	CopyEndToEndHeaders(w.Header(), response.Header)
	w.Header().Set("X-Cache", "BYPASS")
	modified, _ := http.ParseTime(response.Header.Get("Last-Modified"))
	recorder := &contentStatusWriter{ResponseWriter: w, status: http.StatusOK}
	http.ServeContent(recorder, req, req.URL.Path, modified, spool.File)
	return recorder.status, nil
}
