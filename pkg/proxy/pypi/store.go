package pypi

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"os"

	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

type signingState struct {
	Secret string `json:"secret"`
}

func loadSigningSecret(stateDir string) ([]byte, error) {
	var state signingState
	err := storeio.ReadJSON(stateDir, "signing.json", &state)
	if err == nil {
		secret, decodeErr := base64.RawURLEncoding.DecodeString(state.Secret)
		if decodeErr != nil || len(secret) != 32 {
			return nil, errors.New("invalid PyPI signing state")
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
