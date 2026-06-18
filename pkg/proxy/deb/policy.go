package deb

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"gopkg.d7z.net/blobfs"
	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/filerepo"
	"gopkg.d7z.net/cache-proxy/pkg/proxydriver"
)

type Policy = filerepo.Policy
type Rule = filerepo.Rule

type Driver struct{}

func (Driver) Mode() string { return config.ModeDEB }

func (Driver) DecodeJSON(data json.RawMessage) (any, error) { return filerepo.DecodeJSON(data) }

func (Driver) EncodeJSON(policy any) (json.RawMessage, error) { return json.Marshal(policy) }

func (Driver) DecodeYAML(data []byte) (any, error) { return filerepo.DecodeYAML(data) }

func (Driver) EncodeYAML(policy any) ([]byte, error) { return yaml.Marshal(policy) }

func (Driver) ApplyDefaults(spec *proxydriver.ResolvedSpec) {
	filerepo.ApplyDefaults(spec.Policy.(*Policy), config.Freshness(2*time.Minute))
}

func (Driver) Validate(spec *proxydriver.ResolvedSpec) error {
	if len(spec.Source.Upstreams) == 0 {
		return errors.New("deb mode requires at least one upstream")
	}
	return filerepo.Validate(config.ModeDEB, spec.Policy.(*Policy))
}

func (Driver) DefaultFreshFor(spec *proxydriver.ResolvedSpec) config.Freshness { return 0 }

func (Driver) NewHandler(name string, spec *proxydriver.ResolvedSpec, store *blobfs.Store, stats *proxy.Stats) (http.Handler, func(), error) {
	handler, err := filerepo.NewHandler(name, config.ModeDEB, config.ModeDEB, config.Freshness(2*time.Minute), classify, spec.Source, spec.Meta, spec.Policy.(*Policy), store, stats)
	if err != nil {
		return nil, nil, err
	}
	return handler, handler.Close, nil
}

func (Driver) Lookup(spec *proxydriver.ResolvedSpec, lookupPath string) (proxy.Route, error) {
	return filerepo.Lookup(config.ModeDEB, spec.Policy.(*Policy), classify, lookupPath)
}

func classify(cleanPath string) filerepo.ResourceClass {
	switch {
	case strings.HasPrefix(cleanPath, "dists/"):
		return filerepo.ResourceMetadata
	case strings.HasPrefix(cleanPath, "pool/") && (strings.HasSuffix(cleanPath, ".deb") || strings.HasSuffix(cleanPath, ".udeb") || strings.HasSuffix(cleanPath, ".ddeb") || strings.HasSuffix(cleanPath, ".dsc") || strings.Contains(cleanPath, ".orig.tar.") || strings.Contains(cleanPath, ".debian.tar.") || strings.HasSuffix(cleanPath, ".diff.gz")):
		return filerepo.ResourceArtifact
	case strings.HasSuffix(cleanPath, ".gpg"), strings.HasSuffix(cleanPath, ".sig"), strings.HasSuffix(cleanPath, ".asc"), strings.HasSuffix(cleanPath, ".sha256"), strings.HasSuffix(cleanPath, ".sha512"), strings.HasSuffix(cleanPath, ".md5sum"):
		return filerepo.ResourceAuxiliary
	default:
		return filerepo.ResourceAuxiliary
	}
}

func init() {
	proxydriver.Default.Register(Driver{})
}
