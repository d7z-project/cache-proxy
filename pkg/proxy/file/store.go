package file

import (
	"context"
	"net/http"
	"time"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

const objectTenant = "objects"

type storedResponse struct {
	reader  *blobfs.ObjectReader
	headers http.Header
	origin  string
	fetched time.Time
}

func openStored(ctx context.Context, store *blobfs.Store, key string) (*storedResponse, error) {
	object, err := storeio.OpenResponse(ctx, store, objectTenant, key)
	if err != nil {
		return nil, err
	}
	return &storedResponse{reader: object.Reader, headers: object.Header, origin: object.Origin, fetched: object.Fetched}, nil
}
