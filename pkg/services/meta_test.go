package services

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMeta(t *testing.T) {
	dir := t.TempDir()
	meta, err := NewMeta(dir)
	assert.NoError(t, err)
	err = meta.Put("any", "key", "value", false)
	assert.NoError(t, err)
	get, err := meta.Get("any", "key")
	assert.NoError(t, err)
	assert.Equal(t, "value", get)
	err = meta.Put("any", "key", "value", true)
	assert.NoError(t, err)
	_, err = os.Stat(filepath.Join(dir, base64.URLEncoding.EncodeToString([]byte("any"))))
	assert.NoError(t, err)
	gc, err := meta.Gc(0)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(gc))
}
