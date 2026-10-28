package handler_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/barkin94/notification-system/shared/handler"
)

func TestRequestLogger_fields(t *testing.T) {
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))

	opts := handler.HandlerOpts{
		RegisterRoutesFunc: nil,
	}
	h := handler.NewHandler(opts)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/liveness", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)

	var entry map[string]any
	dec := json.NewDecoder(&buf)
	for dec.More() {
		var e map[string]any
		if err := dec.Decode(&e); err != nil {
			t.Fatalf("parse log output: %v", err)
		}
		if e["msg"] == "response" {
			entry = e
		}
	}
	if entry == nil {
		t.Fatal("no response log entry found")
	}

	for _, field := range []string{"time", "level", "msg", "method", "path", "status", "latency_ms"} {
		if _, ok := entry[field]; !ok {
			t.Errorf("missing log field: %s", field)
		}
	}
}
