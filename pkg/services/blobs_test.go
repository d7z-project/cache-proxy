package services

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Blobs(t *testing.T) {
	dir := t.TempDir()
	join := filepath.Join(dir, "blobs")
	blobs, err := NewBlobs(join)
	assert.NoError(t, err)
	update, err := blobs.Update(strings.NewReader("123456789"))
	assert.NoError(t, err)
	hash := "15e2b0d3c33891ebb0f1ef609ec419420c20e320ce94c65fbc8c3312448eb225"
	assert.Equal(t, hash, update)
	stat, err := os.Stat(filepath.Join(join, hash[:4], hash[4:]))
	assert.NoError(t, err)
	assert.True(t, !stat.IsDir())
}
