package health

import (
	"time"
)

type rateBucket struct {
	successes int
	failures  int
}

type rateWindow struct {
	buckets    []rateBucket
	numBuckets int
	head       int
	headSecond int64
}

func newRateWindow(evalWindow time.Duration) *rateWindow {
	n := int(evalWindow / bucketDuration)
	if n < 10 {
		n = 10
	}
	if n > maxBuckets {
		n = maxBuckets
	}
	return &rateWindow{buckets: make([]rateBucket, n), numBuckets: n}
}

func (w *rateWindow) record(success bool) {
	now := time.Now().Unix()
	idx := int(now) % w.numBuckets

	if now > w.headSecond {
		steps := int(now - w.headSecond)
		if steps > w.numBuckets {
			steps = w.numBuckets
		}
		for i := 1; i <= steps; i++ {
			pos := (w.head + i) % w.numBuckets
			w.buckets[pos].successes = 0
			w.buckets[pos].failures = 0
		}
		w.head = idx
		w.headSecond = now
	}

	if success {
		w.buckets[idx].successes++
	} else {
		w.buckets[idx].failures++
	}
}

func (w *rateWindow) errorRate() float64 {
	var success, fail int
	for i := 0; i < w.numBuckets; i++ {
		success += w.buckets[i].successes
		fail += w.buckets[i].failures
	}
	total := success + fail
	if total < minSampleSize {
		return 0
	}
	return float64(fail) / float64(total)
}

func (w *rateWindow) totalSamples() int {
	var n int
	for i := 0; i < w.numBuckets; i++ {
		n += w.buckets[i].successes + w.buckets[i].failures
	}
	return n
}
