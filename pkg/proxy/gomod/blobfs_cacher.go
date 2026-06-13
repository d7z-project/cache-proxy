package gomod

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"
	"time"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

type blobFSCacher struct {
	store       *blobfs.Store
	tenant      string
	expireAfter config.Duration
}

func newBlobFSCacher(store *blobfs.Store, tenant string, expireAfter config.Duration) *blobFSCacher {
	return &blobFSCacher{store: store, tenant: tenant, expireAfter: expireAfter}
}

func (c *blobFSCacher) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	objectPath, err := goObjectPath(name)
	if err != nil {
		return nil, fs.ErrNotExist
	}
	reader, err := c.store.OpenObject(ctx, c.tenant, objectPath)
	if err != nil {
		return nil, err
	}
	info := reader.Info()
	if c.expireAfter > 0 && time.Since(info.UpdatedAt) > c.expireAfter.Duration() {
		_ = reader.Close()
		_ = c.store.DeleteObject(ctx, c.tenant, objectPath)
		return nil, fs.ErrNotExist
	}
	return &cacheReader{ObjectReader: reader}, nil
}

func (c *blobFSCacher) Put(ctx context.Context, name string, content io.ReadSeeker) error {
	objectPath, err := goObjectPath(name)
	if err != nil {
		return err
	}
	if _, err = content.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if parent := path.Dir(objectPath); parent != "." {
		if err = c.store.MkdirAll(c.tenant+"/"+parent, 0o755); err != nil {
			return err
		}
	}
	_, err = c.store.Put(ctx, c.tenant, objectPath, content, map[string]string{
		"mode":       config.ModeGo,
		"fetched-at": time.Now().UTC().Format(time.RFC3339),
		"go-name":    name,
	})
	return err
}

func goObjectPath(name string) (string, error) {
	if name == "" || strings.Contains(name, "\x00") || strings.HasPrefix(name, "/") {
		return "", fmt.Errorf("invalid Go module cache name %q", name)
	}
	for _, part := range strings.Split(name, "/") {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("invalid Go module cache name %q", name)
		}
	}
	name = strings.TrimPrefix(path.Clean("/"+name), "/")
	return "go/" + name, nil
}

type cacheReader struct {
	*blobfs.ObjectReader
}

func (r *cacheReader) Size() int64 {
	return r.Info().Size
}

func (r *cacheReader) LastModified() time.Time {
	return r.Info().UpdatedAt
}

func (r *cacheReader) ModTime() time.Time {
	return r.Info().UpdatedAt
}

func (r *cacheReader) ETag() string {
	hash := r.Info().FileHash
	if hash == "" {
		return ""
	}
	if strings.HasPrefix(hash, `"`) && strings.HasSuffix(hash, `"`) {
		return hash
	}
	return `"` + hash + `"`
}
