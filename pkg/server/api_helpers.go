package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func listInstanceSummaries(instances map[string]config.InstanceSpec) []config.InstanceSummary {
	result := make([]config.InstanceSummary, 0, len(instances))
	for _, name := range sortedInstanceNames(instances) {
		result = append(result, instances[name].Summary())
	}
	return result
}

func collectChanged(instances map[string]config.InstanceSpec) []config.InstanceSpec {
	result := make([]config.InstanceSpec, 0, len(instances))
	for _, name := range sortedInstanceNames(instances) {
		result = append(result, instances[name])
	}
	return result
}

func parseGeneration(req *http.Request) (uint64, error) {
	value := strings.TrimSpace(req.URL.Query().Get("generation"))
	if value == "" {
		return 0, errors.New("generation is required")
	}
	return parseUint(value)
}

func parseUint(value string) (uint64, error) {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, errors.New("invalid generation")
	}
	return parsed, nil
}

func writeJSON(w http.ResponseWriter, value any, err error) {
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func writeConfigError(w http.ResponseWriter, err error) {
	var conflict conflictError
	if errors.As(err, &conflict) {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeError(w, http.StatusBadRequest, err)
}
