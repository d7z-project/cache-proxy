package npm

import (
	"context"
	"io"
	"net/http"
	"time"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

const npmTenant = "npm"

type cachedObject struct {
	reader    *blobfs.ObjectReader
	headers   http.Header
	fetchedAt time.Time
	origin    string
}

func openObject(ctx context.Context, store *blobfs.Store, key string) (*cachedObject, error) {
	object, err := storeio.OpenResponse(ctx, store, npmTenant, key)
	if err != nil {
		return nil, err
	}
	return &cachedObject{reader: object.Reader, headers: object.Header, fetchedAt: object.Fetched, origin: object.Origin}, nil
}

func putObject(ctx context.Context, store *blobfs.Store, key, origin string, headers http.Header, source io.Reader) error {
	return storeio.PutResponse(ctx, store, npmTenant, key, origin, http.StatusOK, headers, "", source)
}

func touchObject(ctx context.Context, store *blobfs.Store, key string) error {
	return storeio.TouchResponse(ctx, store, npmTenant, key, nil)
}
