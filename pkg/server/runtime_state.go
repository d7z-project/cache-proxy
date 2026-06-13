package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"path"
	"time"

	"gopkg.d7z.net/blobfs"
	"gopkg.in/yaml.v3"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	fileproxy "gopkg.d7z.net/cache-proxy/pkg/proxy/file"
	"gopkg.d7z.net/cache-proxy/pkg/proxydriver"
)

func loadOrInitState(ctx context.Context, store *blobfs.Store, registry *proxydriver.Registry, defaultMetricsPath string, defaultGCInterval time.Duration) (*config.GlobalConfig, map[string]config.InstanceSpec, uint64, error) {
	global, instances, generation, err := loadState(ctx, store, registry)
	if err == nil {
		return global, instances, generation, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, nil, 0, err
	}
	global = DefaultGlobalConfig(defaultMetricsPath, defaultGCInterval)
	instances = DefaultInstances()
	if err := writeYAMLObject(ctx, store, systemTenant, globalConfigPath, global); err != nil {
		return nil, nil, 0, err
	}
	for _, spec := range instances {
		if err := writeInstanceSpec(ctx, store, registry, spec); err != nil {
			return nil, nil, 0, err
		}
	}
	if err := writeYAMLObject(ctx, store, systemTenant, instanceIndexPath, buildIndexDocument(instances)); err != nil {
		return nil, nil, 0, err
	}
	if err := writeJSONObject(ctx, store, systemTenant, revisionStatePath, revisionState{Generation: 1}); err != nil {
		return nil, nil, 0, err
	}
	return loadState(ctx, store, registry)
}

func loadState(ctx context.Context, store *blobfs.Store, registry *proxydriver.Registry) (*config.GlobalConfig, map[string]config.InstanceSpec, uint64, error) {
	revision := revisionState{}
	if err := readJSONObject(ctx, store, systemTenant, revisionStatePath, &revision); err != nil {
		return nil, nil, 0, err
	}
	global := &config.GlobalConfig{}
	if err := readYAMLObject(ctx, store, systemTenant, globalConfigPath, global); err != nil {
		return nil, nil, 0, err
	}
	index := instanceIndexDocument{}
	if err := readYAMLObject(ctx, store, systemTenant, instanceIndexPath, &index); err != nil {
		return nil, nil, 0, err
	}
	instances := make(map[string]config.InstanceSpec, len(index.Instances))
	for _, item := range index.Instances {
		spec, err := readInstanceSpec(ctx, store, registry, item.Name)
		if err != nil {
			return nil, nil, 0, err
		}
		instances[item.Name] = spec
	}
	return global, instances, revision.Generation, nil
}

func DefaultGlobalConfig(metricsPath string, gcInterval time.Duration) *config.GlobalConfig {
	return &config.GlobalConfig{
		Version: 1,
		Metrics: config.MetricsConfig{Path: metricsPath},
		Storage: config.StorageConfig{GC: config.GCConfig{Blob: config.Duration(gcInterval)}},
	}
}

func DefaultInstances() map[string]config.InstanceSpec {
	policy, _ := json.Marshal(&fileproxy.Policy{
		DefaultPolicy: config.PolicyBypass,
		BusyPolicy:    config.BusyPolicyBypass,
		Rules: []fileproxy.Rule{
			{Match: "**/*.iso", Policy: config.PolicyImmutable, ExpireAfter: config.Duration(mustDefaultDuration("DefaultExpireAfter", DefaultExpireAfter))},
			{Match: "**/repodata/**", Policy: config.PolicyRevalidate},
		},
	})
	return map[string]config.InstanceSpec{
		"example-files": {
			Name: "example-files",
			Meta: config.InstanceMeta{
				Mode:        config.ModeFile,
				Enabled:     true,
				Description: "Example file proxy",
				ExpireAfter: config.Duration(mustDefaultDuration("DefaultExpireAfter", DefaultExpireAfter)),
			},
			Route:  config.InstanceRoute{Path: "/files"},
			Source: config.InstanceSource{Upstreams: []string{"https://example.com"}},
			Policy: policy,
		},
	}
}

func buildIndexDocument(instances map[string]config.InstanceSpec) instanceIndexDocument {
	items := make([]config.InstanceSummary, 0, len(instances))
	for _, name := range sortedInstanceNames(instances) {
		items = append(items, instances[name].Summary())
	}
	return instanceIndexDocument{Instances: items}
}

func readYAMLObject(ctx context.Context, store *blobfs.Store, tenant, objectPath string, target any) error {
	data, err := readObject(ctx, store, tenant, objectPath)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, target)
}

func writeYAMLObject(ctx context.Context, store *blobfs.Store, tenant, objectPath string, value any) error {
	data, err := yaml.Marshal(value)
	if err != nil {
		return err
	}
	return writeObject(ctx, store, tenant, objectPath, data, map[string]string{"type": "config", "updated-at": time.Now().UTC().Format(time.RFC3339)})
}

func readJSONObject(ctx context.Context, store *blobfs.Store, tenant, objectPath string, target any) error {
	data, err := readObject(ctx, store, tenant, objectPath)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

func writeJSONObject(ctx context.Context, store *blobfs.Store, tenant, objectPath string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return writeObject(ctx, store, tenant, objectPath, data, map[string]string{"type": "config", "updated-at": time.Now().UTC().Format(time.RFC3339)})
}

func readObject(ctx context.Context, store *blobfs.Store, tenant, objectPath string) ([]byte, error) {
	reader, err := store.OpenObject(ctx, tenant, objectPath)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	data, err := io.ReadAll(io.LimitReader(reader, defaultConfigLimit+1))
	if err != nil {
		return nil, err
	}
	if len(data) > defaultConfigLimit {
		return nil, errors.New("config object is too large")
	}
	return data, nil
}

func writeObject(ctx context.Context, store *blobfs.Store, tenant, objectPath string, data []byte, meta map[string]string) error {
	if len(data) > defaultConfigLimit {
		return errors.New("config object is too large")
	}
	if parent := path.Dir(objectPath); parent != "." {
		if err := store.MkdirAll(tenant+"/"+parent, 0o755); err != nil {
			return err
		}
	}
	_, err := store.Put(ctx, tenant, objectPath, bytes.NewReader(data), meta)
	return err
}

func readInstanceSpec(ctx context.Context, store *blobfs.Store, registry *proxydriver.Registry, name string) (config.InstanceSpec, error) {
	meta := config.InstanceMeta{}
	route := config.InstanceRoute{}
	source := config.InstanceSource{}
	if err := readYAMLObject(ctx, store, systemTenant, instanceShardPath(name, "meta.yaml"), &meta); err != nil {
		return config.InstanceSpec{}, err
	}
	if err := readYAMLObject(ctx, store, systemTenant, instanceShardPath(name, "route.yaml"), &route); err != nil {
		return config.InstanceSpec{}, err
	}
	if err := readYAMLObject(ctx, store, systemTenant, instanceShardPath(name, "source.yaml"), &source); err != nil {
		return config.InstanceSpec{}, err
	}
	policyData, err := readObject(ctx, store, systemTenant, instanceShardPath(name, "policy.yaml"))
	if err != nil {
		return config.InstanceSpec{}, err
	}
	resolved, err := registry.ResolveFromYAML(config.InstanceSpec{Name: name, Meta: meta, Route: route, Source: source}, policyData)
	if err != nil {
		return config.InstanceSpec{}, err
	}
	policyJSON, err := resolved.Driver.EncodeJSON(resolved.Policy)
	if err != nil {
		return config.InstanceSpec{}, err
	}
	return config.InstanceSpec{Name: name, Meta: meta, Route: route, Source: source, Policy: policyJSON}, nil
}

func writeInstanceSpec(ctx context.Context, store *blobfs.Store, registry *proxydriver.Registry, spec config.InstanceSpec) error {
	resolved, err := registry.Resolve(spec)
	if err != nil {
		return err
	}
	policyData, err := resolved.Driver.EncodeYAML(resolved.Policy)
	if err != nil {
		return err
	}
	if err := writeYAMLObject(ctx, store, systemTenant, instanceShardPath(spec.Name, "meta.yaml"), resolved.Meta); err != nil {
		return err
	}
	if err := writeYAMLObject(ctx, store, systemTenant, instanceShardPath(spec.Name, "route.yaml"), resolved.Route); err != nil {
		return err
	}
	if err := writeYAMLObject(ctx, store, systemTenant, instanceShardPath(spec.Name, "source.yaml"), resolved.Source); err != nil {
		return err
	}
	return writeObject(ctx, store, systemTenant, instanceShardPath(spec.Name, "policy.yaml"), policyData, map[string]string{"type": "config", "updated-at": time.Now().UTC().Format(time.RFC3339)})
}

func deleteInstanceConfig(ctx context.Context, store *blobfs.Store, name string) error {
	for _, shard := range []string{"meta.yaml", "route.yaml", "source.yaml", "policy.yaml"} {
		if err := store.DeleteObject(ctx, systemTenant, instanceShardPath(name, shard)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return nil
}

func instanceShardPath(name, shard string) string {
	return "config/instances/" + name + "/" + shard
}
