package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/krafton-jungle-project-4team/loop-ad_event_collector/internal/event"
	"github.com/krafton-jungle-project-4team/loop-ad_event_collector/internal/producer"
)

const maxBodyBytes int64 = 256 * 1024

type Producer interface {
	Produce(context.Context, producer.Message) error
}

type Config struct {
	Producer Producer
	Logger   *slog.Logger
}

type Server struct {
	producer Producer
	logger   *slog.Logger
}

func New(cfg Config) *Server {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		producer: cfg.Producer,
		logger:   logger,
	}
}

func (s *Server) Routes() http.Handler {
	router := chi.NewRouter()
	router.Use(corsMiddleware)
	router.Get("/health", s.handleHealth)
	router.Post("/", s.handleIngest)
	router.Options("/", handleOptions)
	router.Post("/events", s.handleIngest)
	router.Options("/events", handleOptions)
	return router
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if err := requireJSONContentType(r); err != nil {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", err.Error())
		return
	}

	body, status, err := readBody(w, r)
	if err != nil {
		writeError(w, status, "bad_request", err.Error())
		return
	}

	requestID := requestIDFrom(r)
	row, value, err := event.NormalizeForClickHouse(body, requestID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}

	if err := s.producer.Produce(r.Context(), producer.Message{Key: []byte(row.EventID), Value: value}); err != nil {
		s.logger.Error("event publish failed", "error", err, "event_id", row.EventID, "request_id", row.RequestID)
		writeError(w, http.StatusServiceUnavailable, "service_unavailable", "event publish failed")
		return
	}

	s.logger.Info("event accepted", "event_id", row.EventID, "request_id", row.RequestID, "event_type", row.EventType)
	writeJSON(w, http.StatusAccepted, map[string]any{
		"accepted":   1,
		"event_id":   row.EventID,
		"request_id": row.RequestID,
	})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		addCORSHeaders(w)
		next.ServeHTTP(w, r)
	})
}

func handleOptions(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

func addCORSHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Request-Id")
	w.Header().Set("Access-Control-Max-Age", "86400")
}

func requireJSONContentType(r *http.Request) error {
	value := r.Header.Get("Content-Type")
	if value == "" {
		return fmt.Errorf("Content-Type must be application/json")
	}
	mediaType, _, err := mime.ParseMediaType(value)
	if err != nil {
		return fmt.Errorf("Content-Type must be application/json")
	}
	if mediaType != "application/json" {
		return fmt.Errorf("Content-Type must be application/json")
	}
	return nil
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, int, error) {
	defer r.Body.Close()

	if r.ContentLength > maxBodyBytes {
		return nil, http.StatusRequestEntityTooLarge, fmt.Errorf("event body is %d bytes, max %d", r.ContentLength, maxBodyBytes)
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes+1))
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return nil, http.StatusRequestEntityTooLarge, fmt.Errorf("event body exceeds max %d bytes", maxBodyBytes)
		}
		return nil, http.StatusBadRequest, fmt.Errorf("event body read failed")
	}
	if len(body) == 0 {
		return nil, http.StatusBadRequest, fmt.Errorf("event body is required")
	}
	if int64(len(body)) > maxBodyBytes {
		return nil, http.StatusRequestEntityTooLarge, fmt.Errorf("event body exceeds max %d bytes", maxBodyBytes)
	}
	return body, http.StatusOK, nil
}

func requestIDFrom(r *http.Request) string {
	if value := r.Header.Get("X-Request-Id"); value != "" {
		return value
	}

	var bytes [8]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "req_unavailable"
	}
	return "req_" + hex.EncodeToString(bytes[:])
}

func writeError(w http.ResponseWriter, status int, code string, message string) {
	writeJSON(w, status, map[string]any{
		"error":   code,
		"message": message,
	})
}

func writeJSON(w http.ResponseWriter, status int, body map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
