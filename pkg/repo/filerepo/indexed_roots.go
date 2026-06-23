package filerepo

import "time"

type RootSpec interface {
	Key() string
	Targets() []MetadataTarget
	Merge(RootSpec) bool
}

type Discoverer interface {
	Discover(cleanPath string) (RootSpec, bool)
}

type RepositoryState string

const (
	RepositoryStatePending    RepositoryState = "pending"
	RepositoryStateActive     RepositoryState = "active"
	RepositoryStateRefreshing RepositoryState = "refreshing"
	RepositoryStateSuspect    RepositoryState = "suspect"
	RepositoryStateRemoved    RepositoryState = "removed"
	RepositoryStateBlocked    RepositoryState = "blocked"
)

type RemovalPolicy struct {
	ConsecutiveNotFound int
	MinNotFoundAge      time.Duration
}

type RepositoryRecord struct {
	Spec                 RootSpec
	State                RepositoryState
	Snapshot             *LiveSnapshot
	LastSeenAt           time.Time
	LastRefreshAt        time.Time
	LastSuccessAt        time.Time
	NextRefreshAt        time.Time
	FirstNotFoundAt      time.Time
	ConsecutiveNotFound  int
	ConsecutiveInvalid   int
	ConsecutiveTransient int
	LastError            string
}

type rootStateRecord struct {
	State                string    `yaml:"state"`
	LastRefreshAt        time.Time `yaml:"last_refresh_at"`
	LastSuccessAt        time.Time `yaml:"last_success_at"`
	ConsecutiveNotFound  int       `yaml:"consecutive_not_found"`
	ConsecutiveInvalid   int       `yaml:"consecutive_invalid"`
	ConsecutiveTransient int       `yaml:"consecutive_transient"`
	LastError            string    `yaml:"last_error,omitempty"`
}

type persistedState struct {
	Version int                       `yaml:"version"`
	Roots   map[string]rootStateRecord `yaml:"roots"`
}

type staticRootSpec struct {
	key     string
	targets []MetadataTarget
}

func (s staticRootSpec) Key() string { return s.key }

func (s staticRootSpec) Targets() []MetadataTarget {
	return append([]MetadataTarget(nil), s.targets...)
}

func (s staticRootSpec) Merge(other RootSpec) bool {
	return false
}
