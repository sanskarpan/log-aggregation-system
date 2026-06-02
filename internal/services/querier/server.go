package querier

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
	"github.com/sanskar/log-aggregation-system/internal/engine/quota"
	"github.com/sanskar/log-aggregation-system/internal/platform/servicehttp"
)

type Server struct {
	desc   servicehttp.Descriptor
	engine QueryEngine
}

type QueryEngine interface {
	Query(context.Context, model.QueryRequest) (model.QueryResult, error)
}

func NewServer(engine QueryEngine) *Server {
	return &Server{desc: Descriptor(), engine: engine}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	servicehttp.RegisterBaseRoutes(mux, s.desc)
	mux.HandleFunc("/internal/v1/execute", s.handleExecute)
	return mux
}

func (s *Server) handleExecute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req model.QueryRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	result, err := s.engine.Query(r.Context(), req)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, quota.ErrQueryConcurrencyExceeded) {
			status = http.StatusTooManyRequests
		}
		servicehttp.WriteJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	servicehttp.WriteJSON(w, http.StatusOK, result)
}

func decodeJSON(body io.ReadCloser, target any) error {
	defer body.Close()
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}
