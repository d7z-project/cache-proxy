package cargo

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"path"
	"strings"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	"gopkg.d7z.net/cache-proxy/pkg/proxy/shared/httpcache"
)

type resolver struct {
	policy *Policy
	store  *blobfs.Store
	name   string
}

const maxCargoIndexLineSize = 2 << 20

func newResolver(policy *Policy, store *blobfs.Store, name string) *resolver {
	return &resolver{policy: policy, store: store, name: name}
}

func (r *resolver) Resolve(req *http.Request) (httpcache.Route, error) {
	lookupPath := strings.TrimPrefix(path.Clean("/"+req.URL.Path), "/")
	if !httpcache.SafePath(lookupPath) {
		return httpcache.Route{}, errors.New("invalid cargo request path")
	}
	if lookupPath == "." || lookupPath == "" {
		lookupPath = "config.json"
	}
	switch {
	case lookupPath == "config.json":
		return httpcache.Route{
			ObjectPath:   "cargo/index/config.json",
			UpstreamPath: "config.json",
			Policy:       config.PolicyRevalidate,
			FreshFor:     r.policy.IndexFreshFor,
			BusyPolicy:   r.policy.IndexBusyPolicy,
			RewriteKind:  "cargo-config",
			AuthRequired: r.policy.AuthRequired,
		}, nil
	case strings.HasPrefix(lookupPath, "api/v1/crates/") && strings.HasSuffix(lookupPath, "/download"):
		objectPath := "cargo/crates/" + strings.TrimPrefix(lookupPath, "api/v1/crates/")
		targetURL := r.crateTargetURL(req.Context(), lookupPath)
		var allowedTargetHosts []string
		if parsed, err := url.Parse(targetURL); err == nil && parsed.Host != "" {
			allowedTargetHosts = []string{parsed.Host}
		}
		return httpcache.Route{
			ObjectPath:         objectPath,
			UpstreamPath:       lookupPath,
			TargetURL:          targetURL,
			AllowedTargetHosts: allowedTargetHosts,
			Policy:             r.policy.CratePolicy,
			BusyPolicy:         config.BusyPolicyJoin,
		}, nil
	default:
		return httpcache.Route{
			ObjectPath:   "cargo/index/" + lookupPath,
			UpstreamPath: lookupPath,
			Policy:       config.PolicyRevalidate,
			FreshFor:     r.policy.IndexFreshFor,
			BusyPolicy:   r.policy.IndexBusyPolicy,
		}, nil
	}
}

func (r *resolver) crateTargetURL(ctx context.Context, upstreamPath string) string {
	reader, err := r.store.OpenObject(ctx, r.name, "cargo/index/config.json")
	if err != nil {
		return ""
	}
	defer func() { _ = reader.Close() }()
	var cfg httpcache.CargoConfig
	if err := json.NewDecoder(reader).Decode(&cfg); err != nil || strings.TrimSpace(cfg.DL) == "" {
		return ""
	}
	parts := strings.SplitN(strings.TrimPrefix(upstreamPath, "api/v1/crates/"), "/", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] != "download" {
		return ""
	}
	downloadURL := strings.TrimRight(strings.TrimSpace(cfg.DL), "/")
	if !strings.Contains(downloadURL, "{crate}") &&
		!strings.Contains(downloadURL, "{version}") &&
		!strings.Contains(downloadURL, "{prefix}") &&
		!strings.Contains(downloadURL, "{lowerprefix}") &&
		!strings.Contains(downloadURL, "{sha256-checksum}") {
		downloadURL += "/{crate}/{version}/download"
	}
	prefix := cratePrefix(parts[0])
	checksum := ""
	if strings.Contains(downloadURL, "{sha256-checksum}") {
		checksum = r.crateChecksum(ctx, parts[0], parts[1])
		if checksum == "" {
			return ""
		}
	}
	targetURL := strings.NewReplacer(
		"{crate}", parts[0],
		"{version}", parts[1],
		"{prefix}", prefix,
		"{lowerprefix}", strings.ToLower(prefix),
		"{sha256-checksum}", checksum,
	).Replace(downloadURL)
	if strings.Contains(targetURL, "{") || config.ValidateHTTPURL(targetURL) != nil {
		return ""
	}
	return targetURL
}

func (r *resolver) crateChecksum(ctx context.Context, crate, version string) string {
	objectPath := path.Join("cargo/index", strings.ToLower(cratePrefix(crate)), strings.ToLower(crate))
	reader, err := r.store.OpenObject(ctx, r.name, objectPath)
	if err != nil {
		return ""
	}
	defer func() { _ = reader.Close() }()

	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64<<10), maxCargoIndexLineSize)
	for scanner.Scan() {
		var entry struct {
			Version  string `json:"vers"`
			Checksum string `json:"cksum"`
		}
		if json.Unmarshal(scanner.Bytes(), &entry) != nil || entry.Version != version {
			continue
		}
		digest, err := hex.DecodeString(entry.Checksum)
		if err == nil && len(digest) == 32 {
			return strings.ToLower(entry.Checksum)
		}
		return ""
	}
	return ""
}

func cratePrefix(name string) string {
	characters := []rune(name)
	switch len(characters) {
	case 0:
		return ""
	case 1:
		return "1"
	case 2:
		return "2"
	case 3:
		return "3/" + string(characters[:1])
	default:
		return string(characters[:2]) + "/" + string(characters[2:4])
	}
}
