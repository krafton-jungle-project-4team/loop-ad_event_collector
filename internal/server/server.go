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

// Producer는 수집 핸들러가 검증된 이벤트를 발행하기 위해 사용하는 최소 인터페이스입니다.
type Producer interface {
	Produce(context.Context, producer.Message) error
}

// Config는 HTTP 서버 생성에 필요한 의존성입니다.
type Config struct {
	Producer Producer
	Logger   *slog.Logger
}

// Server는 HTTP 요청을 검증하고 Kafka 프로듀서로 넘기는 collector 애플리케이션입니다.
type Server struct {
	producer Producer
	logger   *slog.Logger
}

type acceptedResponse struct {
	Accepted int `json:"accepted"`
}

type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// New는 주입된 프로듀서와 logger로 Server를 생성합니다.
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

// Routes는 collector의 HTTP 라우트와 공통 미들웨어를 구성합니다.
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
		router.Use(middleware.RequestSize(maxBodyBytes))
		router.Post("/", s.handleIngest)
		router.Post("/events", s.handleIngest)
		router.Post("/api/event/", s.handleIngest)
		router.Post("/api/event/events", s.handleIngest)
	})
	return router
}

// handleHealth는 로드밸런서와 컨테이너 상태 확인용 응답을 반환합니다.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	render.PlainText(w, r, "ok\n")
}

// handleIngest는 SDK 이벤트 요청을 검증한 뒤 원문 JSON 본문을 Kafka에 발행합니다.
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	body, status, err := readBody(r)
	if err != nil {
		renderError(w, r, status, "bad_request", err.Error())
		return
	}

	requestID := middleware.GetReqID(r.Context())
	if err := event.ValidateSDKPayload(body); err != nil {
		renderError(w, r, http.StatusBadRequest, "bad_request", err.Error())
		return
	}

	if err := s.producer.Produce(r.Context(), producer.Message{Value: body}); err != nil {
		s.logger.Error("event publish failed", "error", err, "request_id", requestID)
		renderError(w, r, http.StatusServiceUnavailable, "service_unavailable", "event publish failed")
		return
	}

	s.logger.Info("event accepted", "request_id", requestID, "body_bytes", len(body))
	render.Status(r, http.StatusAccepted)
	render.JSON(w, r, acceptedResponse{
		Accepted: 1,
	})
}

// readBody는 요청 본문을 읽고 chi RequestSize 제한 초과와 빈 본문을 구분합니다.
func readBody(r *http.Request) ([]byte, int, error) {
	defer r.Body.Close()

	body, err := io.ReadAll(r.Body)
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
	return body, http.StatusOK, nil
}

// renderError는 API 오류 응답 형식을 한 곳에서 맞춥니다.
func renderError(w http.ResponseWriter, r *http.Request, status int, code string, message string) {
	render.Status(r, status)
	render.JSON(w, r, errorResponse{
		Error:   code,
		Message: message,
	})
}
