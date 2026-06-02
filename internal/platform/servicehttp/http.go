package servicehttp

import (
	"encoding/json"
	"net/http"
	"time"
)

type Endpoint struct {
	Method      string `json:"method"`
	Path        string `json:"path"`
	State       string `json:"state"`
	Description string `json:"description"`
}

type Descriptor struct {
	Name         string     `json:"name"`
	Role         string     `json:"role"`
	Version      string     `json:"version"`
	Deployment   []string   `json:"deployment"`
	Dependencies []string   `json:"dependencies"`
	Endpoints    []Endpoint `json:"endpoints"`
}

type statusResponse struct {
	Service   string     `json:"service"`
	Ready     bool       `json:"ready"`
	Timestamp time.Time  `json:"timestamp"`
	Endpoints []Endpoint `json:"endpoints,omitempty"`
}

type stubResponse struct {
	Service string `json:"service"`
	Path    string `json:"path"`
	State   string `json:"state"`
	Message string `json:"message"`
}

func NewHandler(desc Descriptor) http.Handler {
	mux := http.NewServeMux()
	RegisterBaseRoutes(mux, desc)

	for _, endpoint := range desc.Endpoints {
		ep := endpoint
		mux.HandleFunc(ep.Path, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != ep.Method {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}

			WriteJSON(w, http.StatusNotImplemented, stubResponse{
				Service: desc.Name,
				Path:    ep.Path,
				State:   ep.State,
				Message: ep.Description,
			})
		})
	}

	return mux
}

func RegisterBaseRoutes(mux *http.ServeMux, desc Descriptor) {
	if mux == nil {
		return
	}

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		WriteJSON(w, http.StatusOK, map[string]string{
			"service": desc.Name,
			"status":  "ok",
		})
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		WriteJSON(w, http.StatusOK, statusResponse{
			Service:   desc.Name,
			Ready:     true,
			Timestamp: time.Now().UTC(),
		})
	})

	mux.HandleFunc("/info", func(w http.ResponseWriter, _ *http.Request) {
		WriteJSON(w, http.StatusOK, desc)
	})
}

func WriteJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}
