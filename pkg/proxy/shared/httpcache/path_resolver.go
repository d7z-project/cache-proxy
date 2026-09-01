package httpcache

import (
	"errors"
	"net/http"
	"path"
	"strings"
)

type PathClassifier func(string) ObjectClass

type PathResolver struct {
	namespace string
	classify  PathClassifier
}

func NewPathResolver(namespace string, classify PathClassifier) PathResolver {
	return PathResolver{namespace: strings.Trim(namespace, "/"), classify: classify}
}

func (r PathResolver) Resolve(req *http.Request) (Route, error) {
	cleanPath := strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/")
	if cleanPath == "" || cleanPath == "." {
		return Route{}, errors.New("path is required")
	}
	if !SafePath(cleanPath) {
		return Route{}, errors.New("invalid request path")
	}
	class := ClassContent
	if r.classify != nil {
		class = r.classify(cleanPath)
	}
	return Route{
		Class:        class,
		ObjectPath:   path.Join(r.namespace, cleanPath),
		UpstreamPath: cleanPath,
	}, nil
}
