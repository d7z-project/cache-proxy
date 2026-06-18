package rpm

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

func (Driver) Mode() string { return config.ModeRPM }

func (Driver) DecodeJSON(data json.RawMessage) (any, error) { return filerepo.DecodeJSON(data) }

func (Driver) EncodeJSON(policy any) (json.RawMessage, error) { return json.Marshal(policy) }

func (Driver) DecodeYAML(data []byte) (any, error) { return filerepo.DecodeYAML(data) }

func (Driver) EncodeYAML(policy any) ([]byte, error) { return yaml.Marshal(policy) }

func (Driver) ApplyDefaults(spec *proxydriver.ResolvedSpec) {
	filerepo.ApplyDefaults(spec.Policy.(*Policy), config.Freshness(time.Minute))
}

func (Driver) Validate(spec *proxydriver.ResolvedSpec) error {
	if len(spec.Source.Upstreams) == 0 {
		return errors.New("rpm mode requires at least one upstream")
	}
	return filerepo.Validate(config.ModeRPM, spec.Policy.(*Policy))
}

func (Driver) DefaultFreshFor(spec *proxydriver.ResolvedSpec) config.Freshness { return 0 }

func (Driver) NewHandler(name string, spec *proxydriver.ResolvedSpec, store *blobfs.Store, stats *proxy.Stats) (http.Handler, func(), error) {
	handler, err := filerepo.NewHandler(name, config.ModeRPM, config.ModeRPM, config.Freshness(time.Minute), classify, spec.Source, spec.Meta, spec.Policy.(*Policy), store, stats)
	if err != nil {
		return nil, nil, err
	}
	return handler, handler.Close, nil
}

func (Driver) Lookup(spec *proxydriver.ResolvedSpec, lookupPath string) (proxy.Route, error) {
	return filerepo.Lookup(config.ModeRPM, spec.Policy.(*Policy), classify, lookupPath)
}

func classify(cleanPath string) filerepo.ResourceClass {
	switch {
	case strings.HasPrefix(cleanPath, "repodata/"), strings.HasSuffix(cleanPath, "/repomd.xml"), cleanPath == "repomd.xml", strings.HasSuffix(cleanPath, "/mirrorlist"), cleanPath == "mirrorlist", strings.HasSuffix(cleanPath, "/metalink"), cleanPath == "metalink":
		return filerepo.ResourceMetadata
	case strings.HasSuffix(cleanPath, ".rpm"), strings.HasSuffix(cleanPath, ".drpm"):
		return filerepo.ResourceArtifact
	case strings.HasSuffix(cleanPath, ".sig"), strings.HasSuffix(cleanPath, ".asc"), strings.HasSuffix(cleanPath, ".sha256"), strings.HasSuffix(cleanPath, ".sha512"), strings.HasSuffix(cleanPath, ".md5"):
		return filerepo.ResourceAuxiliary
	default:
		return filerepo.ResourceAuxiliary
	}
}

func init() {
	proxydriver.Default.Register(Driver{})
}
