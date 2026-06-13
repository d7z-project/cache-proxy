package web

import (
	"io/fs"
	"sync"
	"testing"
)

func TestAssetsReturnsStableFileSystemAcrossConcurrentCalls(t *testing.T) {
	const workers = 32
	var wg sync.WaitGroup
	results := make(chan fs.FS, workers)

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- Assets()
		}()
	}

	wg.Wait()
	close(results)

	var first fs.FS
	for filesystem := range results {
		if first == nil {
			first = filesystem
			continue
		}
		if filesystem != first {
			t.Fatal("Assets returned different filesystem instances")
		}
	}
}
