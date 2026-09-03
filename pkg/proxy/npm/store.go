package npm

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"os"
	"time"

	"gopkg.d7z.net/blobfs"

	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

const npmTenant = "npm"

type signingState struct {
	Secret string `json:"secret"`
}

type cachedObject struct {
	reader    *blobfs.ObjectReader
	headers   http.Header
	fetchedAt time.Time
	origin    string
}

func loadSigningSecret(stateDir string) ([]byte, error) {
	var state signingState
	err := storeio.ReadJSON(stateDir, "signing.json", &state)
	if err == nil {
		secret, err := base64.RawURLEncoding.DecodeString(state.Secret)
		if err != nil || len(secret) != 32 {
			return nil, errors.New("invalid npm signing secret")
		}
		return secret, nil
	}
	if !os.IsNotExist(err) {
		return nil, err
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, err
	}
	if err := storeio.WriteJSON(stateDir, "signing.json", signingState{Secret: base64.RawURLEncoding.EncodeToString(secret)}); err != nil {
		return nil, err
	}
	return secret, nil
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
