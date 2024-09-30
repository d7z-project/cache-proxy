package services

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMeta(t *testing.T) {
	for _, path := range []string{"any", "path/to/any"} {
		t.Run(fmt.Sprintf("TestMeta[%s]", path), func(t *testing.T) {
			dir := t.TempDir()
			meta, err := NewFileMeta(dir)
			assert.NoError(t, err)
			_, err = meta.GetMeta(path)
			assert.Error(t, err)
			_, err = meta.GetLastUpdate(path)
			assert.Error(t, err)
			err = meta.Put(path, map[string]string{"key": "value"}, false)
			assert.NoError(t, err)
			get, err := meta.Get(path, "key")
			assert.NoError(t, err)
			assert.Equal(t, "value", get)
			err = meta.Put(path, map[string]string{"key": "value"}, true)
			assert.NoError(t, err)
			_, err = os.Stat(filepath.Join(dir, path))
			assert.NoError(t, err)
			gc, err := meta.Gc(func(s string) time.Duration {
				return 0
			})
			assert.NoError(t, err)
			_, err = os.Stat(filepath.Join(dir, path))
			assert.Error(t, err)
			assert.Equal(t, 1, len(gc))
		})
	}
}
