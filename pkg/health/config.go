package health

import (
	"errors"
	"time"
)

type Config struct {
	Enabled bool `yaml:"enabled"`

	ResourceRemoveAge   time.Duration `yaml:"resource_remove_age"`
	ResourceRemoveCount int           `yaml:"resource_remove_count"`

	evaluationWindow time.Duration
}

type ConfigPatch struct {
	Enabled *bool `yaml:"enabled,omitempty"`

	ResourceRemoveAge   *time.Duration `yaml:"resource_remove_age,omitempty"`
	ResourceRemoveCount *int           `yaml:"resource_remove_count,omitempty"`
}

const (
	defaultEvaluationWindow = 2 * time.Minute
	ewmaAlpha               = 0.20
	bucketDuration          = time.Second
	maxBuckets              = 600
)

func DefaultConfig() Config {
	return Config{
		Enabled:             true,
		ResourceRemoveAge:   5 * time.Minute,
		ResourceRemoveCount: 5,
		evaluationWindow:    defaultEvaluationWindow,
	}
}

func ApplyConfigPatch(cfg Config, patch *ConfigPatch) Config {
	if patch == nil {
		return cfg
	}
	if patch.Enabled != nil {
		cfg.Enabled = *patch.Enabled
	}
	if patch.ResourceRemoveAge != nil {
		cfg.ResourceRemoveAge = *patch.ResourceRemoveAge
	}
	if patch.ResourceRemoveCount != nil {
		cfg.ResourceRemoveCount = *patch.ResourceRemoveCount
	}
	return cfg
}

func ValidateConfig(cfg Config) error {
	if cfg.ResourceRemoveCount <= 0 {
		return errors.New("health resource_remove_count must be positive")
	}
	if cfg.ResourceRemoveAge < 0 {
		return errors.New("health resource_remove_age must be non-negative")
	}
	return nil
}
