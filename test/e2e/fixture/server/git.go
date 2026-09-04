package main

import (
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

func (s *fixtureServer) serveGit(w http.ResponseWriter, r *http.Request, revision fixtureRevision) {
	command := exec.Command("git", "http-backend")
	command.Env = append(os.Environ(),
		"GIT_PROJECT_ROOT="+filepath.Join(s.root, revision.directory, "git"),
		"GIT_HTTP_EXPORT_ALL=1",
		"PATH_INFO="+strings.TrimPrefix(r.URL.Path, "/git"),
		"QUERY_STRING="+r.URL.RawQuery,
		"REQUEST_METHOD="+r.Method,
		"CONTENT_TYPE="+r.Header.Get("Content-Type"),
		"CONTENT_LENGTH="+strconv.FormatInt(r.ContentLength, 10),
		"REMOTE_USER=",
		"REMOTE_ADDR=127.0.0.1",
	)
	command.Stdin = r.Body
	output, err := command.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			log.Printf("git http-backend failed: %s", strings.TrimSpace(string(exitErr.Stderr)))
		}
		http.Error(w, "git backend failed", http.StatusInternalServerError)
		return
	}
	headerBlock, body, ok := strings.Cut(string(output), "\r\n\r\n")
	if !ok {
		http.Error(w, "invalid git backend response", http.StatusInternalServerError)
		return
	}
	status := http.StatusOK
	for _, line := range strings.Split(headerBlock, "\r\n") {
		name, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		value = strings.TrimSpace(value)
		if strings.EqualFold(name, "Status") {
			fields := strings.Fields(value)
			if len(fields) != 0 {
				status, _ = strconv.Atoi(fields[0])
			}
			continue
		}
		w.Header().Add(name, value)
	}
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		_, _ = io.WriteString(w, body)
	}
}
