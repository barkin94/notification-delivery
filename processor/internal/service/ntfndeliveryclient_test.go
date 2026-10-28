package service_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/barkin94/notification-system/processor/internal/service"
)

func newTestDeliveryClient(serverURL string) service.NtfnDeliveryClient {
	return service.NewNtfnDeliveryClient(serverURL, 5*time.Second)
}

func TestSend_202_success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	result := newTestDeliveryClient(srv.URL).Send(context.Background(), "+1", "sms", "test")
	if !result.Success {
		t.Error("expected Success=true")
	}
	if result.Retryable {
		t.Error("expected Retryable=false")
	}
}

func TestSend_400_nonRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	result := newTestDeliveryClient(srv.URL).Send(context.Background(), "+1", "sms", "test")
	if result.Success {
		t.Error("expected Success=false")
	}
	if result.Retryable {
		t.Error("expected Retryable=false for 400")
	}
}

func TestSend_401_nonRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	result := newTestDeliveryClient(srv.URL).Send(context.Background(), "+1", "sms", "test")
	if result.Retryable {
		t.Error("expected Retryable=false for 401")
	}
}

func TestSend_403_nonRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	result := newTestDeliveryClient(srv.URL).Send(context.Background(), "+1", "sms", "test")
	if result.Retryable {
		t.Error("expected Retryable=false for 403")
	}
}

func TestSend_503_retryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	result := newTestDeliveryClient(srv.URL).Send(context.Background(), "+1", "sms", "test")
	if !result.Retryable {
		t.Error("expected Retryable=true for 503")
	}
}

func TestSend_429_retryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	result := newTestDeliveryClient(srv.URL).Send(context.Background(), "+1", "sms", "test")
	if !result.Retryable {
		t.Error("expected Retryable=true for 429")
	}
}

func TestSend_timeout_retryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
	}))
	defer srv.Close()

	client := service.NewNtfnDeliveryClient(srv.URL, 50*time.Millisecond)
	result := client.Send(context.Background(), "+1", "sms", "test")
	if !result.Retryable {
		t.Error("expected Retryable=true on timeout")
	}
}

func TestSend_latency_measured(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Millisecond)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	result := newTestDeliveryClient(srv.URL).Send(context.Background(), "+1", "sms", "test")
	if result.LatencyMS <= 0 {
		t.Errorf("expected LatencyMS > 0, got %d", result.LatencyMS)
	}
}
