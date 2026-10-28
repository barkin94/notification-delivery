package httpclient_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/barkin94/notification-system/shared/httpclient"
)

func TestNew_defaultTimeout(t *testing.T) {
	c := httpclient.New("http://example.com")
	assert.Equal(t, 10*time.Second, c.HTTP().Timeout)
}

func TestNew_withTimeout(t *testing.T) {
	c := httpclient.New("http://example.com", httpclient.WithTimeout(5*time.Second))
	assert.Equal(t, 5*time.Second, c.HTTP().Timeout)
}

func TestRequest_success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	c := httpclient.New(srv.URL)
	resp, err := c.Request(context.Background(), http.MethodPost, "/notify", map[string]string{"key": "val"})
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusAccepted, resp.StatusCode)
}

func TestRequest_nilPayload(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(t, r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := httpclient.New(srv.URL)
	resp, err := c.Request(context.Background(), http.MethodGet, "/health", nil)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestRequest_networkError(t *testing.T) {
	c := httpclient.New("http://127.0.0.1:0") // nothing listening
	_, err := c.Request(context.Background(), http.MethodGet, "/", nil)
	require.Error(t, err)
}
