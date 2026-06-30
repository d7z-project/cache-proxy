package scheduler

type taskHeap []*taskState

func (h taskHeap) Len() int           { return len(h) }
func (h taskHeap) Less(i, j int) bool { return h[i].NextRun.Before(h[j].NextRun) }
func (h taskHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *taskHeap) Push(x any) {
	ts := x.(*taskState)
	ts.index = len(*h)
	*h = append(*h, ts)
}

func (h *taskHeap) Pop() any {
	old := *h
	n := len(old)
	ts := old[n-1]
	old[n-1] = nil
	ts.index = -1
	*h = old[0 : n-1]
	return ts
}
