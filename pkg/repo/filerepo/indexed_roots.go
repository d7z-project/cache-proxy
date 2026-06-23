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
