package runtime

import "sync"

type SpoolBudget struct {
	mu    sync.Mutex
	limit int64
	used  int64
}

type SpoolReservation struct {
	budget *SpoolBudget
	bytes  int64
	once   sync.Once
}

func NewSpoolBudget(limit int64) *SpoolBudget {
	return &SpoolBudget{limit: limit}
}

func (b *SpoolBudget) TryReserve(bytes int64) (*SpoolReservation, bool) {
	if b == nil || bytes <= 0 {
		return &SpoolReservation{}, true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if bytes > b.limit || b.used > b.limit-bytes {
		return nil, false
	}
	b.used += bytes
	return &SpoolReservation{budget: b, bytes: bytes}, true
}

func (r *SpoolReservation) Release() {
	if r == nil || r.budget == nil {
		return
	}
	r.once.Do(func() {
		r.budget.mu.Lock()
		r.budget.used -= r.bytes
		r.budget.mu.Unlock()
	})
}

func (b *SpoolBudget) Usage() (used, limit int64) {
	if b == nil {
		return 0, 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used, b.limit
}
