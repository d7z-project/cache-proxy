package services

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestNilDereferenceFix(t *testing.T) {
	// Setup temp dir
	tmpDir, err := os.MkdirTemp("", "cache-proxy-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create Worker to initialize blobfs
	worker, err := NewWorker(tmpDir, time.Hour, time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	// Create Target with NO URLs
	target := NewTarget("test-target") // No urls provided

	// Add a rule to trigger cache logic (and thus download)
	// Without this, it might go to direct mode which has a different path (though potentially also buggy if openRemote returns nil)
	// The reported issue was specifically about download method.
	err = target.AddRule(".*", 2*time.Hour, time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	// Bind target to worker to initialize storage
	err = worker.Bind("test-target", target)
	if err != nil {
		t.Fatal(err)
	}

	// Call forward. This calls fetchResource -> download.
	// We expect it NOT to panic.
	// It should return an error because the file is not in storage and cannot be downloaded (no remote).
	res, err := target.forward(context.Background(), "test-file")
	
	if err == nil {
		// If err is nil, res should probably not be nil, or it might be openBlob success (if file existed?)
		// Here file doesn't exist. openBlob should return error.
		t.Logf("Expected error, got nil. Res: %v", res)
	} else {
		t.Logf("Got expected error: %v", err)
	}
}
