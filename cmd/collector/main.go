package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/krafton-jungle-project-4team/loop-ad_event_collector/internal/config"
	"github.com/krafton-jungle-project-4team/loop-ad_event_collector/internal/producer"
	"github.com/krafton-jungle-project-4team/loop-ad_event_collector/internal/server"
)

// main은 설정 검증, Kafka 프로듀서 생성, HTTP 서버 실행을 순서대로 수행합니다.
func main() {
	healthcheckURL := flag.String("healthcheck", "", "check a health URL and exit")
	flag.Parse()

	if *healthcheckURL != "" {
		os.Exit(checkHealth(*healthcheckURL))
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		fatal(logger, err)
	}

	kafkaProducer, err := producer.NewKafka(producer.KafkaConfig{
		Brokers:  cfg.KafkaBootstrapBrokers,
		Topic:    cfg.EventTopic,
		Username: cfg.KafkaUsername,
		Password: cfg.KafkaPassword,
	})
	if err != nil {
		fatal(logger, err)
	}
	defer func() {
		if err := kafkaProducer.Close(); err != nil {
			logger.Error("kafka producer close failed", "error", err)
		}
	}()

	app := server.New(server.Config{
		Producer: kafkaProducer,
		Logger:   logger,
	})

	httpServer := &http.Server{
		Addr:              cfg.ListenAddr(),
		Handler:           app.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       330 * time.Second,
		MaxHeaderBytes:    16 * 1024,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("event collector listening", "service", cfg.ServiceID, "addr", cfg.ListenAddr(), "topic", cfg.EventTopic)
		errCh <- httpServer.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			fatal(logger, err)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("http server shutdown failed", "error", err)
		}
	}
}

// checkHealth는 컨테이너 상태 확인용 URL을 호출하고 종료 코드로 결과를 반환합니다.
func checkHealth(url string) int {
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return 0
	}

	fmt.Fprintf(os.Stderr, "healthcheck status=%d\n", resp.StatusCode)
	return 1
}

// fatal은 복구할 수 없는 시작 실패를 로그로 남기고 프로세스를 종료합니다.
func fatal(logger *slog.Logger, err error) {
	logger.Error("event collector failed", "error", err)
	os.Exit(1)
}
