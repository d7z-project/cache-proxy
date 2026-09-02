package pypi

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"

	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

type signingState struct {
	Version int    `json:"version"`
	Secret  string `json:"secret"`
}

func loadSigningSecret(stateDir string) ([]byte, error) {
	var state signingState
	err := storeio.ReadJSON(stateDir, "signing.json", &state)
	if err == nil {
		secret, decodeErr := base64.RawURLEncoding.DecodeString(state.Secret)
		if state.Version != 1 || decodeErr != nil || len(secret) != 32 {
			return nil, fmt.Errorf("invalid PyPI signing state")
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
	if err := storeio.WriteJSON(stateDir, "signing.json", signingState{Version: 1, Secret: base64.RawURLEncoding.EncodeToString(secret)}); err != nil {
		return nil, err
	}
	return secret, nil
}
