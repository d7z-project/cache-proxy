package services

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMeta(t *testing.T) {
	dir := t.TempDir()
	meta, err := NewFileMeta(dir)
	assert.NoError(t, err)
	err = meta.Put("any", map[string]string{"key": "value"}, false)
	assert.NoError(t, err)
	get, err := meta.Get("any", "key")
	assert.NoError(t, err)
	assert.Equal(t, "value", get)
	err = meta.Put("any", map[string]string{"key": "value"}, true)
	assert.NoError(t, err)
	_, err = os.Stat(filepath.Join(dir, "any"))
	assert.NoError(t, err)
	gc, err := meta.Gc(0, func(s string) bool {
		return true
	})
	assert.NoError(t, err)
	_, err = os.Stat(filepath.Join(dir, "any"))
	assert.Error(t, err)
	assert.Equal(t, 1, len(gc))
}
