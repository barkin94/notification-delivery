package handler_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/barkin94/notification-system/shared/handler"
)

func TestLiveness_200(t *testing.T) {
	r := handler.NewHandler(handler.HandlerOpts{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/liveness", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestReadiness_200_noCheckers(t *testing.T) {
	r := handler.NewHandler(handler.HandlerOpts{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/readiness", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestReadiness_503_failingChecker(t *testing.T) {
	checkers := []handler.ReadinessChecker{
		{
			Name:  "db",
			Check: func(_ context.Context) error { return errors.New("connection refused") },
		},
	}
	r := handler.NewHandler(handler.HandlerOpts{ReadinessChecks: checkers})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/readiness", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}
}

func TestReadiness_200_passingChecker(t *testing.T) {
	checkers := []handler.ReadinessChecker{
		{
			Name:  "db",
			Check: func(_ context.Context) error { return nil },
		},
	}
	r := handler.NewHandler(handler.HandlerOpts{ReadinessChecks: checkers})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/readiness", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestHealth_routesRegistered(t *testing.T) {
	r := handler.NewHandler(handler.HandlerOpts{})
	for _, path := range []string{"/api/v1/liveness", "/api/v1/readiness"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code == http.StatusNotFound {
			t.Errorf("GET %s returned 404 — route not registered", path)
		}
	}
}
