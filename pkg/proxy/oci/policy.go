package oci

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	containername "github.com/google/go-containerregistry/pkg/name"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

type Options struct {
	Auth *AuthConfig `json:"auth,omitempty" yaml:"auth,omitempty"`
}

type AuthConfig struct {
	Type     string `json:"type" yaml:"type"`
	Username string `json:"username,omitempty" yaml:"username,omitempty"`
	Password string `json:"password,omitempty" yaml:"password,omitempty"`
	Token    string `json:"token,omitempty" yaml:"token,omitempty"`
}

type Block struct {
	Upstream    string
	Transport   *config.TransportConfig
	MetadataTTL time.Duration
	WorkDir     string
	Spooler     *storeio.Spooler
	Options
}

const (
	metadataTTL   = 2 * time.Minute
	blobRetention = config.DefaultRetention
)

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }
func (Driver) Mode() string              { return config.ModeOCI }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var options Options
	if err := plan.Decode(&options); err != nil {
		return err
	}
	upstream := plan.Upstream()
	if err := validateConfig(upstream, &options); err != nil {
		return fmt.Errorf("instance %s: %w", plan.Name(), err)
	}
	if !plan.Enabled() {
		return nil
	}
	handler := newHandler(plan.Name(), Block{
		Upstream: upstream, Transport: plan.Transport(), MetadataTTL: metadataTTL, WorkDir: filepath.Join(plan.StoreRoot(), "work"),
		Spooler: storeio.NewSpooler(filepath.Join(plan.StoreRoot(), "work"), plan.MaxCacheObjectSize(), plan.SpoolBudget()), Options: options,
	}, blobRetention, plan.Store(), plan.Stats(), plan.UpstreamGate())
	if plan.DisplayURL() != "" {
		plan.SetHomeDisplayURL(plan.DisplayURL())
	}
	plan.Scheduler().Register(scheduler.TaskDef{
		Key:      scheduler.NewTaskKey(plan.Name(), scheduler.TypeExpireCleanup, ""),
		Interval: 6 * time.Hour,
		Handler: func(ctx context.Context) (*scheduler.TaskOutcome, error) {
			more, err := handler.Cleanup(ctx, plan.CleanupConfig())
			outcome := &scheduler.TaskOutcome{Result: "success"}
			if more {
				outcome.ContinueAfter = 2 * time.Second
			}
			return outcome, err
		},
	})
	return plan.BindAddr(plan.Bind(), handler)
}

func validateConfig(upstream string, options *Options) error {
	if err := config.ValidateHTTPUpstream(upstream); err != nil {
		return fmt.Errorf("oci upstream URL is invalid: %w", err)
	}
	host := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(upstream, "https://"), "http://"))
	if host != "" {
		host = strings.Split(host, "/")[0]
		if _, err := containername.NewRegistry(host); err != nil {
			return fmt.Errorf("invalid oci registry %q: %w", host, err)
		}
	}
	if options.Auth == nil {
		return nil
	}
	switch strings.ToLower(options.Auth.Type) {
	case "", "none":
		options.Auth = nil
	case "basic":
		options.Auth.Username = os.ExpandEnv(options.Auth.Username)
		options.Auth.Password = os.ExpandEnv(options.Auth.Password)
		if options.Auth.Username == "" || options.Auth.Password == "" {
			return errors.New("oci basic auth requires username and password")
		}
	case "bearer":
		options.Auth.Token = os.ExpandEnv(options.Auth.Token)
		if options.Auth.Token == "" {
			return errors.New("oci bearer auth requires token")
		}
	default:
		return fmt.Errorf("unsupported oci auth type %q", options.Auth.Type)
	}
	return nil
}
