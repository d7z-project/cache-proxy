package cargo

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"

	"gopkg.d7z.net/cache-proxy/pkg/storeio"
)

type registryState struct {
	Download     string `json:"download"`
	AuthRequired bool   `json:"auth_required"`
}

type crateState struct {
	Name      string            `json:"name"`
	Checksums map[string]string `json:"checksums"`
}

func stateName(scope string) string {
	return filepath.ToSlash(filepath.Join("registries", scope, "config.json"))
}

func crateStateName(scope, name string) string {
	digest := sha256.Sum256([]byte(name))
	return filepath.ToSlash(filepath.Join("registries", scope, "crates", hex.EncodeToString(digest[:])+".json"))
}

func loadRegistryState(stateDir, scope string) (registryState, error) {
	var state registryState
	if err := storeio.ReadJSON(stateDir, stateName(scope), &state); err != nil {
		return registryState{}, err
	}
	if state.Download == "" {
		return registryState{}, errors.New("invalid cargo registry state")
	}
	return state, nil
}

func loadCrateState(stateDir, scope, name string) (crateState, error) {
	var state crateState
	if err := storeio.ReadJSON(stateDir, crateStateName(scope, name), &state); err != nil {
		return crateState{}, err
	}
	if state.Name != name || state.Checksums == nil {
		return crateState{}, errors.New("invalid cargo crate state")
	}
	return state, nil
}
