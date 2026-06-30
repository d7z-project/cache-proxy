package git

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/go-git/go-git/v5/plumbing/transport"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/spf13/afero"

	"gopkg.d7z.net/cache-proxy/pkg/config"
	proxyruntime "gopkg.d7z.net/cache-proxy/pkg/runtime"
	"gopkg.d7z.net/cache-proxy/pkg/scheduler"
)

type AuthConfig struct {
	Type     string `yaml:"type"`     // basic | token
	Username string `yaml:"username"` // basic mode
	Password string `yaml:"password"` // basic password or token mode token
}

type Block struct {
	Upstream         string          `yaml:"upstream"`
	Auth             *AuthConfig     `yaml:"auth,omitempty"`
	Proxy            string          `yaml:"proxy,omitempty"`
	SyncInterval     config.Duration `yaml:"sync_interval"`
	OperationTimeout config.Duration `yaml:"operation_timeout"`
	Overwrite        *bool           `yaml:"force_overwrite"`
	Route            struct {
		Path string `yaml:"path"`
	} `yaml:"route"`
}

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }

func (Driver) Mode() string { return config.ModeGit }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var block Block
	if err := plan.Decode(&block); err != nil {
		return err
	}
	if block.Upstream == "" {
		return fmt.Errorf("instance %s: upstream is required", plan.Name())
	}
	if block.Route.Path == "" {
		return fmt.Errorf("instance %s: route.path is required", plan.Name())
	}

	auth, err := buildAuth(block.Auth)
	if err != nil {
		return fmt.Errorf("instance %s: auth: %w", plan.Name(), err)
	}

	var proxyURLStr string
	if block.Proxy != "" {
		if _, err := url.Parse(block.Proxy); err != nil {
			return fmt.Errorf("instance %s: proxy URL: %w", plan.Name(), err)
		}
		proxyURLStr = block.Proxy
	}

	forceOverwrite := true
	if block.Overwrite != nil {
		forceOverwrite = *block.Overwrite
	}

	baseFs := afero.NewBasePathFs(plan.Store(), "git/"+plan.Name())
	billyFs := newBillyAdapter(baseFs, "")

	handler := newGitHandler(gitConfig{
		name:             plan.Name(),
		billyFs:          billyFs,
		upstream:         block.Upstream,
		auth:             auth,
		proxyURL:         proxyURLStr,
		syncInterval:     time.Duration(block.SyncInterval),
		operationTimeout: block.OperationTimeout.Duration(),
		forceOverwrite:   forceOverwrite,
	})

	plan.SetHomeSnippet(plan.RenderSnippet())
	plan.SetHomeDisplayURL(block.Upstream)
	plan.Scheduler().Register(scheduler.TaskDef{
		Key:      scheduler.NewTaskKey(plan.Name(), scheduler.TypeExpireCleanup, ""),
		Interval: 6 * time.Hour,
		Handler: func(ctx context.Context) error {
			return handler.Cleanup(ctx, config.DefaultCleanupConfig())
		},
	})
	return plan.BindPath(block.Route.Path, config.DefaultExpireAfter, handler)
}

func buildAuth(cfg *AuthConfig) (transport.AuthMethod, error) {
	if cfg == nil || cfg.Type == "" {
		return nil, nil
	}
	switch strings.ToLower(cfg.Type) {
	case "basic":
		return &githttp.BasicAuth{
			Username: os.ExpandEnv(cfg.Username),
			Password: os.ExpandEnv(cfg.Password),
		}, nil
	case "token":
		return &githttp.TokenAuth{
			Token: os.ExpandEnv(cfg.Password),
		}, nil
	default:
		return nil, fmt.Errorf("unknown auth type %q, expected basic or token", cfg.Type)
	}
}
