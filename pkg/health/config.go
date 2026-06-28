package health

import "time"

type Config struct {
	Enabled bool `yaml:"enabled"`

	DegradeRate      float64       `yaml:"degrade_rate"`
	TripRate         float64       `yaml:"trip_rate"`
	EvaluationWindow time.Duration `yaml:"evaluation_window"`

	DegradeLatency time.Duration `yaml:"degrade_latency"`
	MinWeight      float64       `yaml:"min_weight"`

	CanaryCooldown time.Duration `yaml:"canary_cooldown"`
	CanaryStep     float64       `yaml:"canary_step"`

	ProbeInterval time.Duration `yaml:"probe_interval"`
	ProbeTimeout  time.Duration `yaml:"probe_timeout"`

	ResourceBlockInterval time.Duration `yaml:"resource_block_interval"`
	ResourceRemoveAge     time.Duration `yaml:"resource_remove_age"`
	ResourceRemoveCount   int           `yaml:"resource_remove_count"`
}

type ConfigPatch struct {
	Enabled *bool `yaml:"enabled,omitempty"`

	DegradeRate      *float64       `yaml:"degrade_rate,omitempty"`
	TripRate         *float64       `yaml:"trip_rate,omitempty"`
	EvaluationWindow *time.Duration `yaml:"evaluation_window,omitempty"`

	DegradeLatency *time.Duration `yaml:"degrade_latency,omitempty"`
	MinWeight      *float64       `yaml:"min_weight,omitempty"`

	CanaryCooldown *time.Duration `yaml:"canary_cooldown,omitempty"`
	CanaryStep     *float64       `yaml:"canary_step,omitempty"`

	ProbeInterval *time.Duration `yaml:"probe_interval,omitempty"`
	ProbeTimeout  *time.Duration `yaml:"probe_timeout,omitempty"`

	ResourceBlockInterval *time.Duration `yaml:"resource_block_interval,omitempty"`
	ResourceRemoveAge     *time.Duration `yaml:"resource_remove_age,omitempty"`
	ResourceRemoveCount   *int           `yaml:"resource_remove_count,omitempty"`
}

const (
	minSampleSize     = 10
	ewmaAlpha         = 0.20
	canaryCeiling     = 0.50
	canarySuccessMin  = 3
	maxDynamicPaths   = 8
	resourceFailCount = 5
	bucketDuration    = time.Second
	maxBuckets        = 600
)

func DefaultConfig() Config {
	return Config{
		Enabled: true,

		DegradeRate:      0.1,
		TripRate:         0.3,
		EvaluationWindow: 2 * time.Minute,

		DegradeLatency: 2 * time.Second,
		MinWeight:      0.1,

		CanaryCooldown: 30 * time.Second,
		CanaryStep:     0.1,

		ProbeInterval: 2 * time.Minute,
		ProbeTimeout:  5 * time.Second,

		ResourceBlockInterval: 2 * time.Minute,
		ResourceRemoveAge:     5 * time.Minute,
		ResourceRemoveCount:   5,
	}
}

func ApplyConfigPatch(cfg Config, patch *ConfigPatch) Config {
	if patch == nil {
		return cfg
	}
	if patch.Enabled != nil {
		cfg.Enabled = *patch.Enabled
	}
	if patch.DegradeRate != nil {
		cfg.DegradeRate = *patch.DegradeRate
	}
	if patch.TripRate != nil {
		cfg.TripRate = *patch.TripRate
	}
	if patch.EvaluationWindow != nil {
		cfg.EvaluationWindow = *patch.EvaluationWindow
	}
	if patch.DegradeLatency != nil {
		cfg.DegradeLatency = *patch.DegradeLatency
	}
	if patch.MinWeight != nil {
		cfg.MinWeight = *patch.MinWeight
	}
	if patch.CanaryCooldown != nil {
		cfg.CanaryCooldown = *patch.CanaryCooldown
	}
	if patch.CanaryStep != nil {
		cfg.CanaryStep = *patch.CanaryStep
	}
	if patch.ProbeInterval != nil {
		cfg.ProbeInterval = *patch.ProbeInterval
	}
	if patch.ProbeTimeout != nil {
		cfg.ProbeTimeout = *patch.ProbeTimeout
	}
	if patch.ResourceBlockInterval != nil {
		cfg.ResourceBlockInterval = *patch.ResourceBlockInterval
	}
	if patch.ResourceRemoveAge != nil {
		cfg.ResourceRemoveAge = *patch.ResourceRemoveAge
	}
	if patch.ResourceRemoveCount != nil {
		cfg.ResourceRemoveCount = *patch.ResourceRemoveCount
	}
	return cfg
}
