package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"gopkg.d7z.net/cache-proxy/pkg/config"
)

func (r *Runtime) instancesCollectionAPI(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		r.mu.RLock()
		instances := config.CloneInstances(r.instances)
		generation := r.generation
		r.mu.RUnlock()
		writeJSON(w, instancesCollectionResponse{
			Generation: generation,
			Items:      listInstanceSummaries(req, instances),
		}, nil)
	case http.MethodPost:
		var input saveInstanceRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, req.Body, int64(defaultConfigLimit))).Decode(&input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if input.Spec == nil {
			writeError(w, http.StatusBadRequest, errors.New("spec is required"))
			return
		}
		spec := config.CloneInstance(*input.Spec)
		r.mu.RLock()
		global := config.CloneGlobal(r.global)
		instances := config.CloneInstances(r.instances)
		r.mu.RUnlock()
		if _, exists := instances[spec.Name]; exists {
			writeError(w, http.StatusConflict, errors.New("instance already exists"))
			return
		}
		instances[spec.Name] = spec
		nextGeneration, err := r.replaceState(req.Context(), input.Generation, global, instances, []config.InstanceSpec{spec}, nil)
		if err != nil {
			writeConfigError(w, err)
			return
		}
		writeJSON(w, instanceDocumentResponse{Generation: nextGeneration, Spec: spec}, nil)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (r *Runtime) instanceDocumentAPI(w http.ResponseWriter, req *http.Request, name string) {
	if name == "" || containsPathSeparator(name) {
		http.NotFound(w, req)
		return
	}
	switch req.Method {
	case http.MethodGet:
		r.mu.RLock()
		spec, ok := r.instances[name]
		generation := r.generation
		r.mu.RUnlock()
		if !ok {
			writeError(w, http.StatusNotFound, errors.New("instance not found"))
			return
		}
		writeJSON(w, instanceDocumentResponse{Generation: generation, Spec: config.CloneInstance(spec)}, nil)
	case http.MethodPut:
		var input saveInstanceRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, req.Body, int64(defaultConfigLimit))).Decode(&input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if input.Spec == nil {
			writeError(w, http.StatusBadRequest, errors.New("spec is required"))
			return
		}
		spec := config.CloneInstance(*input.Spec)
		if spec.Name != name {
			writeError(w, http.StatusBadRequest, errors.New("instance name cannot be changed"))
			return
		}
		r.mu.RLock()
		global := config.CloneGlobal(r.global)
		instances := config.CloneInstances(r.instances)
		r.mu.RUnlock()
		if _, exists := instances[name]; !exists {
			writeError(w, http.StatusNotFound, errors.New("instance not found"))
			return
		}
		instances[name] = spec
		nextGeneration, err := r.replaceState(req.Context(), input.Generation, global, instances, []config.InstanceSpec{spec}, nil)
		if err != nil {
			writeConfigError(w, err)
			return
		}
		writeJSON(w, instanceDocumentResponse{Generation: nextGeneration, Spec: spec}, nil)
	case http.MethodDelete:
		generation, err := parseGeneration(req)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		r.mu.RLock()
		global := config.CloneGlobal(r.global)
		instances := config.CloneInstances(r.instances)
		r.mu.RUnlock()
		if _, exists := instances[name]; !exists {
			writeError(w, http.StatusNotFound, errors.New("instance not found"))
			return
		}
		delete(instances, name)
		nextGeneration, replaceErr := r.replaceState(req.Context(), generation, global, instances, nil, []string{name})
		if replaceErr != nil {
			writeConfigError(w, replaceErr)
			return
		}
		writeJSON(w, deleteInstanceResponse{Generation: nextGeneration, Deleted: name}, nil)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (r *Runtime) instancesExportAPI(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	r.mu.RLock()
	items := make([]config.InstanceSpec, 0, len(r.instances))
	for _, name := range sortedInstanceNames(r.instances) {
		items = append(items, config.CloneInstance(r.instances[name]))
	}
	response := exportResponse{
		Generation: r.generation,
		Global:     config.CloneGlobal(r.global),
		Instances:  items,
	}
	r.mu.RUnlock()
	writeJSON(w, response, nil)
}

func (r *Runtime) instancesImportAPI(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var input importInstancesRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, req.Body, int64(defaultConfigLimit))).Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if len(input.Instances) == 0 {
		writeError(w, http.StatusBadRequest, errors.New("no instances to import"))
		return
	}
	r.mu.RLock()
	global := config.CloneGlobal(r.global)
	instances := config.CloneInstances(r.instances)
	r.mu.RUnlock()
	changed := make([]config.InstanceSpec, 0, len(input.Instances))
	for _, item := range input.Instances {
		spec := config.CloneInstance(item)
		if spec.Name == "" {
			writeError(w, http.StatusBadRequest, errors.New("instance name is required"))
			return
		}
		if _, exists := instances[spec.Name]; exists && !input.Replace {
			writeError(w, http.StatusConflict, errors.New("instance already exists: "+spec.Name))
			return
		}
		instances[spec.Name] = spec
		changed = append(changed, spec)
	}
	nextGeneration, err := r.replaceState(req.Context(), input.Generation, global, instances, changed, nil)
	if err != nil {
		writeConfigError(w, err)
		return
	}
	writeJSON(w, importInstancesResponse{Generation: nextGeneration, Imported: len(changed)}, nil)
}

func containsPathSeparator(value string) bool {
	for _, r := range value {
		if r == '/' {
			return true
		}
	}
	return false
}
