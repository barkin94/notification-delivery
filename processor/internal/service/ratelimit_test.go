package service_test

import (
	"context"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/barkin94/notification-system/processor/internal/service"
)

// redisAddr is set by TestMain when Docker is available; empty otherwise.
var redisAddr string

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := tcredis.Run(ctx,
		"redis:7-alpine",
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("6379/tcp").WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		// Docker unavailable — ratelimit integration tests will be skipped;
		// all other service unit tests run normally.
		log.Printf("Redis container unavailable, skipping ratelimit integration tests: %v", err)
		os.Exit(m.Run())
	}
	defer container.Terminate(ctx) //nolint:errcheck

	redisAddr, err = container.Endpoint(ctx, "")
	if err != nil {
		log.Fatalf("get redis endpoint: %v", err)
	}

	os.Exit(m.Run())
}

// requireRedis skips the calling test when Docker/Redis is not available.
func requireRedis(t *testing.T) {
	t.Helper()
	if redisAddr == "" {
		t.Skip("Redis not available (Docker not running)")
	}
}

func newRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: redisAddr})
}

func TestLimiter_allows(t *testing.T) {
	requireRedis(t)
	ctx := context.Background()
	limiter := service.NewLimiter(newRedisClient(), nil)

	for i := 0; i < 100; i++ {
		ok, _, err := limiter.IsAllowed(ctx, "sms")
		if err != nil {
			t.Fatalf("IsAllowed error at i=%d: %v", i, err)
		}
		if !ok {
			t.Fatalf("expected allowed at i=%d", i)
		}
	}
}

func TestLimiter_throttles(t *testing.T) {
	requireRedis(t)
	ctx := context.Background()
	limiter := service.NewLimiter(newRedisClient(), nil)

	denied := 0
	for i := 0; i < 500; i++ {
		ok, _, err := limiter.IsAllowed(ctx, "email")
		if err != nil {
			t.Fatalf("Allow error: %v", err)
		}
		if !ok {
			denied++
		}
	}
	if denied == 0 {
		t.Error("expected some requests to be denied when burst exceeded")
	}
}

func TestLimiter_refills(t *testing.T) {
	requireRedis(t)
	ctx := context.Background()
	limiter := service.NewLimiter(newRedisClient(), nil)

	for i := 0; i < 500; i++ {
		_, _, _ = limiter.IsAllowed(ctx, "push")
	}

	ok, _, err := limiter.IsAllowed(ctx, "push")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected denied after bucket exhausted")
	}

	time.Sleep(100 * time.Millisecond)

	ok, _, err = limiter.IsAllowed(ctx, "push")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected allowed after refill window")
	}
}

func TestLimiter_atomic(t *testing.T) {
	requireRedis(t)
	ctx := context.Background()
	limiter := service.NewLimiter(newRedisClient(), nil)

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, _, err := limiter.IsAllowed(ctx, "concurrent")
			if err != nil {
				t.Errorf("Allow error: %v", err)
				return
			}
			if ok {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()

	if allowed.Load() != 50 {
		t.Errorf("expected 50 allowed, got %d", allowed.Load())
	}
}
