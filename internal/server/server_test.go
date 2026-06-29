package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/krafton-jungle-project-4team/loop-ad_event_collector/internal/producer"
)

func TestHealthReturnsOK(t *testing.T) {
	app := New(Config{Producer: &fakeProducer{}})
	resp := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)

	app.Routes().ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusOK)
	}
	if resp.Body.String() != "ok\n" {
		t.Fatalf("body = %q", resp.Body.String())
	}
}

func TestIngestAcceptsSDKPayloadAtRoot(t *testing.T) {
	producer := &fakeProducer{}
	app := New(Config{Producer: producer})
	body := sdkPayload

	resp := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://demo-shoppingmall.dev.loop-ad.org")
	req.Header.Set("X-Request-Id", "req_001")

	app.Routes().ServeHTTP(resp, req)

	if resp.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", resp.Code, resp.Body.String())
	}
	if len(producer.messages) != 1 {
		t.Fatalf("messages = %d, want 1", len(producer.messages))
	}
	if len(producer.messages[0].Key) != 0 {
		t.Fatalf("message key = %q", producer.messages[0].Key)
	}
	if string(producer.messages[0].Value) != body {
		t.Fatalf("message value = %s", producer.messages[0].Value)
	}
	if resp.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("missing cors header")
	}
}

func TestIngestAcceptsEventsPath(t *testing.T) {
	app := New(Config{Producer: &fakeProducer{}})

	resp := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(sdkPayload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-Id", "req_001")

	app.Routes().ServeHTTP(resp, req)

	if resp.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", resp.Code, resp.Body.String())
	}
}

func TestIngestAcceptsPublicApiEventPaths(t *testing.T) {
	paths := []string{"/api/event/", "/api/event/events"}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			app := New(Config{Producer: &fakeProducer{}})

			resp := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(sdkPayload))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Request-Id", "req_001")

			app.Routes().ServeHTTP(resp, req)

			if resp.Code != http.StatusAccepted {
				t.Fatalf("status = %d, body = %s", resp.Code, resp.Body.String())
			}
		})
	}
}

func TestOptionsReturnsNoContentForIngestPath(t *testing.T) {
	app := New(Config{Producer: &fakeProducer{}})
	resp := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/api/event/events", nil)
	req.Header.Set("Origin", "https://demo-shoppingmall.dev.loop-ad.org")
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)

	app.Routes().ServeHTTP(resp, req)

	if resp.Code < 200 || resp.Code >= 300 {
		t.Fatalf("status = %d, want 2xx", resp.Code)
	}
}

func TestIngestRejectsInvalidSDKPayload(t *testing.T) {
	app := New(Config{Producer: &fakeProducer{}})
	resp := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"event_id":"evt_001"}`))
	req.Header.Set("Content-Type", "application/json")

	app.Routes().ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusBadRequest)
	}
}

func TestIngestRejectsUnsupportedContentType(t *testing.T) {
	app := New(Config{Producer: &fakeProducer{}})
	resp := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "text/plain")

	app.Routes().ServeHTTP(resp, req)

	if resp.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusUnsupportedMediaType)
	}
}

func TestIngestRejectsOversizedBody(t *testing.T) {
	app := New(Config{Producer: &fakeProducer{}})
	resp := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(strings.Repeat("x", int(maxBodyBytes)+1)))
	req.Header.Set("Content-Type", "application/json")

	app.Routes().ServeHTTP(resp, req)

	if resp.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusRequestEntityTooLarge)
	}
}

func TestIngestReturnsUnavailableWhenProducerFails(t *testing.T) {
	app := New(Config{Producer: &fakeProducer{err: errors.New("kafka down")}})
	resp := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(sdkPayload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-Id", "req_001")

	app.Routes().ServeHTTP(resp, req)

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusServiceUnavailable)
	}
}

type fakeProducer struct {
	messages []producer.Message
	err      error
}

func (f *fakeProducer) Produce(_ context.Context, message producer.Message) error {
	if f.err != nil {
		return f.err
	}
	f.messages = append(f.messages, message)
	return nil
}

const sdkPayload = `{
	"project_id":"demo-shoppingmall",
	"event_id":"evt_001",
	"user_id":"u_001",
	"session_id":"s_001",
	"event_time":"2026-06-27T10:00:00.000Z",
	"event_name":"page_view",
	"channel":"demo",
	"campaign_id":"cmp_001",
	"age_group":"30s",
	"gender":"male",
	"device":"mobile",
	"category":"Home/Eco-Friendly",
	"product_id":"GGOEGCBD142299",
	"inventory_status":"in_stock",
	"price":12900,
	"quantity":1,
	"revenue":12900,
	"coupon_id":"",
	"order_id":"",
	"experiment_id":"",
	"variant_id":"",
	"action_id":"",
	"mapping_id":"",
	"ad_id":"",
	"creative_id":"cr_001",
	"bandit_policy_id":"",
	"bandit_arm_id":"",
	"bandit_decision_id":"",
	"reward_value":0,
	"properties_json":"{\"page\":{\"path\":\"/products/sku-1\"},\"sdk\":{\"name\":\"loop-ad_event_sdk\"}}"
}`
