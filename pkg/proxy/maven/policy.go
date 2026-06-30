package maven

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
)

const defaultCleanupInterval = 6 * time.Hour

type Policy struct {
	MetadataFreshFor   config.Freshness `json:"metadataFreshFor,omitempty" yaml:"metadata_fresh_for,omitempty"`
	MetadataBusyPolicy string           `json:"metadataBusyPolicy,omitempty" yaml:"metadata_busy_policy,omitempty"`
	ChecksumPolicy     string           `json:"checksumPolicy,omitempty" yaml:"checksum_policy,omitempty"`
	ChecksumFreshFor   config.Freshness `json:"checksumFreshFor,omitempty" yaml:"checksum_fresh_for,omitempty"`
	ChecksumBusyPolicy string           `json:"checksumBusyPolicy,omitempty" yaml:"checksum_busy_policy,omitempty"`
	ReleasePolicy      string           `json:"releasePolicy,omitempty" yaml:"release_policy,omitempty"`
	SnapshotPolicy     string           `json:"snapshotPolicy,omitempty" yaml:"snapshot_policy,omitempty"`
	SnapshotFreshFor   config.Freshness `json:"snapshotFreshFor,omitempty" yaml:"snapshot_fresh_for,omitempty"`
}

type Block struct {
	ExpireAfter config.Expiration `yaml:"expire_after"`
	Route       struct {
		Path string `yaml:"path"`
	} `yaml:"route"`
	Upstream  string                  `yaml:"upstream"`
	Transport *config.TransportConfig `yaml:"transport,omitempty"`
	Policy    `yaml:",inline"`
}

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }
func (Driver) Mode() string              { return config.ModeMaven }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var block Block
	if err := plan.Decode(&block); err != nil {
		return err
	}
	if strings.TrimSpace(block.Upstream) == "" {
		return fmt.Errorf("instance %s: maven mode requires one upstream", plan.Name())
	}
	if _, err := url.Parse(block.Upstream); err != nil {
		return fmt.Errorf("instance %s: maven upstream URL is invalid: %w", plan.Name(), err)
	}
	applyDefaults(&block.Policy)
	if err := validate(&block.Policy); err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	expireAfter := block.ExpireAfter
	if expireAfter.IsUnset() {
		expireAfter = config.DefaultExpireAfter
	}
	handler := httpcache.NewHandler(plan.Name(), httpcache.RuntimeConfig{
		Mode:            config.ModeMaven,
		ExpireAfter:     expireAfter,
		Upstreams:       []string{strings.TrimSpace(block.Upstream)},
		Transport:       block.Transport,
		BusyPolicy:      config.BusyPolicyBypass,
		DownloadLimiter: plan.Downloads(),
	}, plan.Store(), newResolver(&block.Policy), plan.Stats(), nil)
	plan.Scheduler().Register(scheduler.TaskDef{
		Key:      scheduler.NewTaskKey(plan.Name(), scheduler.TypeExpireCleanup, ""),
		Interval: defaultCleanupInterval,
		Handler: func(ctx context.Context) error {
			return handler.Cleanup(ctx, config.DefaultCleanupConfig())
		},
	})
	plan.SetHomeSnippet(plan.RenderSnippet())
	return plan.BindPath(block.Route.Path, expireAfter, proxyruntime.HandlerInstance{
		Handler:      handler,
		Close:        func() error { handler.Close(); return nil },
		CloseContext: handler.CloseContext,
		CleanupFn:    handler.Cleanup,
	})
}

func applyDefaults(policy *Policy) {
	if policy.MetadataBusyPolicy == "" {
		policy.MetadataBusyPolicy = config.BusyPolicyStale
	}
	if policy.ReleasePolicy == "" {
		policy.ReleasePolicy = config.PolicyImmutable
	}
	if policy.ChecksumPolicy == "" {
		policy.ChecksumPolicy = config.PolicyRevalidate
	}
	if policy.ChecksumFreshFor == 0 {
		policy.ChecksumFreshFor = config.Freshness(30 * time.Second)
	}
	if policy.ChecksumBusyPolicy == "" {
		policy.ChecksumBusyPolicy = config.BusyPolicyBypass
	}
	if policy.SnapshotPolicy == "" {
		policy.SnapshotPolicy = config.PolicyRevalidate
	}
}

func validate(policy *Policy) error {
	if !config.ValidPolicy(policy.ReleasePolicy) {
		return fmt.Errorf("invalid maven release policy %q", policy.ReleasePolicy)
	}
	if !config.ValidPolicy(policy.SnapshotPolicy) {
		return fmt.Errorf("invalid maven snapshot policy %q", policy.SnapshotPolicy)
	}
	if !config.ValidPolicy(policy.ChecksumPolicy) {
		return fmt.Errorf("invalid maven checksum policy %q", policy.ChecksumPolicy)
	}
	if !config.ValidBusyPolicy(policy.MetadataBusyPolicy) {
		return fmt.Errorf("invalid maven metadata busy policy %q", policy.MetadataBusyPolicy)
	}
	if !config.ValidBusyPolicy(policy.ChecksumBusyPolicy) {
		return fmt.Errorf("invalid maven checksum busy policy %q", policy.ChecksumBusyPolicy)
	}
	if policy.MetadataFreshFor > 0 && policy.MetadataFreshFor.Duration() < time.Second {
		return fmt.Errorf("maven metadata fresh_for must be at least 1s")
	}
	if policy.ChecksumFreshFor > 0 && policy.ChecksumFreshFor.Duration() < time.Second {
		return fmt.Errorf("maven checksum fresh_for must be at least 1s")
	}
	if policy.SnapshotFreshFor > 0 && policy.SnapshotFreshFor.Duration() < time.Second {
		return fmt.Errorf("maven snapshot fresh_for must be at least 1s")
	}
	return nil
}

type resolver struct{ policy *Policy }

func newResolver(policy *Policy) *resolver { return &resolver{policy: policy} }

func (r *resolver) Resolve(req *http.Request) (httpcache.Route, error) {
	lookupPath := strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/")
	if !httpcache.SafePath(lookupPath) {
		return httpcache.Route{}, fmt.Errorf("invalid maven request path")
	}
	if lookupPath == "." || lookupPath == "" {
		return httpcache.Route{}, fmt.Errorf("path is required")
	}
	route := httpcache.Route{
		ObjectPath:   "maven/" + lookupPath,
		UpstreamPath: lookupPath,
		Policy:       r.defaultPolicy(lookupPath),
	}
	if isMetadataPath(lookupPath) {
		route.Policy = config.PolicyRevalidate
		route.FreshFor = r.policy.MetadataFreshFor
		route.BusyPolicy = r.policy.MetadataBusyPolicy
		return route, nil
	}
	if isAuxiliaryPath(lookupPath) {
		route.Policy = r.policy.ChecksumPolicy
		route.FreshFor = r.policy.ChecksumFreshFor
		route.BusyPolicy = r.policy.ChecksumBusyPolicy
	} else if isSnapshotPath(lookupPath) {
		route.Policy = r.policy.SnapshotPolicy
		route.FreshFor = r.policy.SnapshotFreshFor
	}
	return route, nil
}

func (r *resolver) defaultPolicy(lookupPath string) string {
	if isSnapshotPath(lookupPath) {
		return r.policy.SnapshotPolicy
	}
	return r.policy.ReleasePolicy
}

func isSnapshotPath(lookupPath string) bool {
	return strings.Contains(lookupPath, "-SNAPSHOT") || strings.Contains(lookupPath, "/SNAPSHOT/")
}

func isMetadataPath(lookupPath string) bool {
	return strings.HasSuffix(lookupPath, "maven-metadata.xml")
}

func isAuxiliaryPath(lookupPath string) bool {
	if strings.Contains(lookupPath, "maven-metadata.xml.") {
		return true
	}
	for _, suffix := range []string{".sha1", ".sha256", ".sha512", ".md5", ".asc", ".sig"} {
		if strings.HasSuffix(lookupPath, suffix) {
			return true
		}
	}
	return false
}
