package health

import "time"

type Config struct {
	Enabled bool `yaml:"enabled"`
}

type ConfigPatch struct {
	Enabled *bool `yaml:"enabled,omitempty"`
}

const (
	defaultEvaluationWindow = 2 * time.Minute
	ewmaAlpha               = 0.20
	bucketDuration          = time.Second
	maxBuckets              = 600
)

func DefaultConfig() Config {
	return Config{
		Enabled: true,
	}
}

func ApplyConfigPatch(cfg Config, patch *ConfigPatch) Config {
	if patch == nil {
		return cfg
	}
	if patch.Enabled != nil {
		cfg.Enabled = *patch.Enabled
	}
	return cfg
}
