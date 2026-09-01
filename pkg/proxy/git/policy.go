package git

import (
	"context"
	"errors"
	"fmt"
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
	Auth             *AuthConfig     `yaml:"auth,omitempty"`
	SyncInterval     config.Duration `yaml:"sync_interval"`
	OperationTimeout config.Duration `yaml:"operation_timeout"`
}

type Driver struct{}

func NewDriver() proxyruntime.ModeDriver { return Driver{} }

func (Driver) Mode() string { return config.ModeGit }

func (Driver) Plan(_ context.Context, plan *proxyruntime.InstancePlan) error {
	var block Block
	if err := plan.Decode(&block); err != nil {
		return err
	}
	if block.SyncInterval < 0 {
		return fmt.Errorf("instance %s: git sync_interval must not be negative", plan.Name())
	}
	if block.OperationTimeout < 0 {
		return fmt.Errorf("instance %s: git operation_timeout must not be negative", plan.Name())
	}
	upstream := strings.TrimSpace(plan.Upstreams()[0])
	if _, err := transport.NewEndpoint(upstream); err != nil {
		return fmt.Errorf("instance %s: invalid git upstream: %w", plan.Name(), err)
	}

	auth, err := buildAuth(block.Auth)
	if err != nil {
		return fmt.Errorf("instance %s: auth: %w", plan.Name(), err)
	}

	var proxyURL string
	if plan.Transport() != nil {
		proxyURL = plan.Transport().Proxy
	}
	if !plan.Enabled() {
		return nil
	}

	baseFs := afero.NewBasePathFs(plan.Store(), "git/"+plan.Name())
	billyFs := newBillyAdapter(baseFs, "")

	handler := newGitHandler(gitConfig{
		name:             plan.Name(),
		billyFs:          billyFs,
		upstream:         upstream,
		auth:             auth,
		proxyURL:         proxyURL,
		operationTimeout: block.OperationTimeout.Duration(),
		upstreamGate:     plan.UpstreamGate(),
	})

	plan.SetHomeDisplayURL(upstream)
	syncInterval := block.SyncInterval.Duration()
	if syncInterval <= 0 {
		syncInterval = 5 * time.Minute
	}
	plan.Scheduler().Register(scheduler.TaskDef{
		Key:            scheduler.NewTaskKey(plan.Name(), scheduler.TypeGitSync, ""),
		Interval:       syncInterval,
		RunImmediately: true,
		Handler: func(ctx context.Context) (*scheduler.TaskOutcome, error) {
			return nil, handler.Sync(ctx)
		},
	})
	return plan.BindPath(plan.Path(), plan.Retention(), handler)
}

func buildAuth(cfg *AuthConfig) (transport.AuthMethod, error) {
	if cfg == nil || cfg.Type == "" {
		return nil, nil
	}
	switch strings.ToLower(cfg.Type) {
	case "basic":
		username := os.ExpandEnv(cfg.Username)
		password := os.ExpandEnv(cfg.Password)
		if username == "" || password == "" {
			return nil, errors.New("basic auth requires username and password")
		}
		return &githttp.BasicAuth{
			Username: username,
			Password: password,
		}, nil
	case "token":
		token := os.ExpandEnv(cfg.Password)
		if token == "" {
			return nil, errors.New("token auth requires password")
		}
		return &githttp.TokenAuth{
			Token: token,
		}, nil
	default:
		return nil, fmt.Errorf("unknown auth type %q, expected basic or token", cfg.Type)
	}
}
