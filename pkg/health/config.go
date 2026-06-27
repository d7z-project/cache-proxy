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
