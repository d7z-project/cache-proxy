package oci

import (
	"errors"
	"net/http"
	"path"
	"strings"

	"github.com/bmatcuk/doublestar/v4"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

type requestKind uint8

const (
	requestInvalid requestKind = iota
	requestPing
	requestTags
	requestManifest
	requestBlob
	requestBypass
)

type request struct {
	kind         requestKind
	repo         string
	ref          string
	digest       string
	upstreamPath string
	match        repoMatch
}

type repoMatch struct {
	policy      string
	busyPolicy  string
	expireAfter config.Expiration
}

func resolveRequest(req *http.Request, cfg *Policy) (request, error) {
	cleanPath := strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/")
	if cleanPath == "v2" || cleanPath == "v2/" {
		return request{kind: requestPing, upstreamPath: "v2"}, nil
	}
	if !httpcache.SafePath(cleanPath) || !strings.HasPrefix(cleanPath, "v2/") {
		return request{}, errors.New("invalid OCI request path")
	}
	parts := strings.Split(cleanPath, "/")
	for i, part := range parts {
		if part == "blobs" && i+2 < len(parts) && parts[i+1] == "uploads" {
			return request{}, errors.New("OCI blob uploads are not proxied")
		}
		if part == "manifests" && i+1 < len(parts) {
			repo := strings.Join(parts[1:i], "/")
			ref := parts[i+1]
			if repo == "" || ref == "" {
				return request{}, errors.New("invalid OCI manifest path")
			}
			return request{
				kind:         requestManifest,
				repo:         repo,
				ref:          ref,
				upstreamPath: cleanPath,
				match:        matchRepo(cfg, repo),
			}, nil
		}
		if part == "blobs" && i+1 < len(parts) {
			repo := strings.Join(parts[1:i], "/")
			digest := parts[i+1]
			if repo == "" || digest == "" || !strings.Contains(digest, ":") {
				return request{}, errors.New("invalid OCI blob path")
			}
			return request{
				kind:         requestBlob,
				repo:         repo,
				digest:       digest,
				upstreamPath: cleanPath,
				match:        matchRepo(cfg, repo),
			}, nil
		}
		if part == "tags" && i+1 < len(parts) && parts[i+1] == "list" {
			repo := strings.Join(parts[1:i], "/")
			return request{
				kind:         requestTags,
				repo:         repo,
				upstreamPath: cleanPath,
				match:        matchRepo(cfg, repo),
			}, nil
		}
	}
	return request{kind: requestBypass, upstreamPath: cleanPath}, nil
}

func matchRepo(cfg *Policy, repoName string) repoMatch {
	if cfg == nil {
		return repoMatch{policy: config.PolicyBypass, busyPolicy: config.BusyPolicyBypass}
	}
	for _, rule := range cfg.Rules {
		if !doublestar.MatchUnvalidated(rule.Match, repoName) {
			continue
		}
		match := repoMatch{
			policy:      rule.Policy,
			busyPolicy:  cfg.BusyPolicy,
			expireAfter: rule.ExpireAfter,
		}
		if match.policy == "" {
			match.policy = config.PolicyBypass
		}
		return match
	}
	match := repoMatch{
		policy:      cfg.DefaultPolicy,
		busyPolicy:  cfg.BusyPolicy,
		expireAfter: 0,
	}
	if match.policy == "" {
		match.policy = config.PolicyBypass
	}
	if match.busyPolicy == "" {
		match.busyPolicy = config.BusyPolicyBypass
	}
	return match
}

func parseOCIRef(ref string) (string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", errors.New("empty OCI reference")
	}
	if i := strings.LastIndex(ref, ":"); i >= 0 && !strings.Contains(ref[i+1:], "/") {
		repo, tag := ref[:i], ref[i+1:]
		if repo != "" && tag != "" {
			return "v2/" + repo + "/manifests/" + tag, nil
		}
	}
	return "v2/" + ref + "/tags/list", nil
}
