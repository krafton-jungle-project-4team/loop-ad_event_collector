package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/go-chi/render"

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

type acceptedResponse struct {
	Accepted  int    `json:"accepted"`
	EventID   string `json:"event_id"`
	RequestID string `json:"request_id"`
}

type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
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
	router.Use(middleware.RequestID)
	router.Use(cors.Handler(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{http.MethodPost, http.MethodOptions},
		AllowedHeaders: []string{"Content-Type", "X-Request-Id"},
		MaxAge:         86400,
	}))
	router.Get("/health", s.handleHealth)
	router.Group(func(router chi.Router) {
		router.Use(middleware.AllowContentType("application/json"))
		router.Post("/", s.handleIngest)
		router.Post("/events", s.handleIngest)
	})
	return router
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	render.PlainText(w, r, "ok\n")
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	body, status, err := readBody(w, r)
	if err != nil {
		renderError(w, r, status, "bad_request", err.Error())
		return
	}

	requestID := middleware.GetReqID(r.Context())
	row, value, err := event.NormalizeForClickHouse(body, requestID)
	if err != nil {
		renderError(w, r, http.StatusBadRequest, "bad_request", err.Error())
		return
	}

	if err := s.producer.Produce(r.Context(), producer.Message{Key: []byte(row.EventID), Value: value}); err != nil {
		s.logger.Error("event publish failed", "error", err, "event_id", row.EventID, "request_id", row.RequestID)
		renderError(w, r, http.StatusServiceUnavailable, "service_unavailable", "event publish failed")
		return
	}

	s.logger.Info("event accepted", "event_id", row.EventID, "request_id", row.RequestID, "event_type", row.EventType)
	render.Status(r, http.StatusAccepted)
	render.JSON(w, r, acceptedResponse{
		Accepted:  1,
		EventID:   row.EventID,
		RequestID: row.RequestID,
	})
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

func renderError(w http.ResponseWriter, r *http.Request, status int, code string, message string) {
	render.Status(r, status)
	render.JSON(w, r, errorResponse{
		Error:   code,
		Message: message,
	})
}
