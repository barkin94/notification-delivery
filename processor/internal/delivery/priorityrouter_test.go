package delivery_test

import (
	"context"
	"testing"
	"time"

	apipub "github.com/barkin94/notification-system/api/public"
	"github.com/barkin94/notification-system/processor/internal/delivery"
	natsmsg "github.com/barkin94/notification-system/shared/messaging/nats"
)

// result aliases the concrete type to keep test lines short.
type result = natsmsg.Result[apipub.NotificationReadyEvent]

func makeResult(priority string) result {
	return result{Event: apipub.NotificationReadyEvent{Priority: priority}}
}

func filledChan(results []result) <-chan result {
	ch := make(chan result, len(results))
	for _, r := range results {
		ch <- r
	}
	return ch
}

func emptyChan() <-chan result {
	return make(chan result)
}

// newRouter is a convenience wrapper that mirrors the old 3-arg signature used
// throughout the tests, so each call site stays concise.
// Weights are passed explicitly — the router package owns no defaults.
func newRouter(high, normal, low <-chan result, weights [3]int) *delivery.PriorityRouter[result] {
	return delivery.NewPriorityRouter([]delivery.WeightedSource[result]{
		{Ch: high, Weight: weights[0]},
		{Ch: normal, Weight: weights[1]},
		{Ch: low, Weight: weights[2]},
	})
}

func TestWeightedRatio(t *testing.T) {
	const total = 600
	highMsgs := make([]result, 300)
	normalMsgs := make([]result, 200)
	lowMsgs := make([]result, 100)
	for i := range highMsgs {
		highMsgs[i] = makeResult("high")
	}
	for i := range normalMsgs {
		normalMsgs[i] = makeResult("normal")
	}
	for i := range lowMsgs {
		lowMsgs[i] = makeResult("low")
	}

	r := newRouter(filledChan(highMsgs), filledChan(normalMsgs), filledChan(lowMsgs), [3]int{3, 2, 1})

	counts := map[string]int{}
	ctx := context.Background()
	for i := 0; i < total; i++ {
		res, ok := r.Next(ctx)
		if !ok {
			t.Fatalf("Next returned false at iteration %d", i)
		}
		counts[res.Event.Priority]++
	}

	assertRatio(t, "high", counts["high"], total, 3.0/6.0, 0.05)
	assertRatio(t, "normal", counts["normal"], total, 2.0/6.0, 0.05)
	assertRatio(t, "low", counts["low"], total, 1.0/6.0, 0.05)
}

func assertRatio(t *testing.T, name string, got, total int, want, tol float64) {
	t.Helper()
	ratio := float64(got) / float64(total)
	if ratio < want-tol || ratio > want+tol {
		t.Errorf("%s ratio = %.3f, want %.3f ±%.3f", name, ratio, want, tol)
	}
}

func TestFallbackHighEmpty(t *testing.T) {
	r := newRouter(
		emptyChan(),
		filledChan([]result{makeResult("normal")}),
		emptyChan(),
		[3]int{3, 2, 1},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	res, ok := r.Next(ctx)
	if !ok {
		t.Fatal("expected a result, got false")
	}
	if res.Event.Priority != "normal" {
		t.Errorf("expected normal, got %s", res.Event.Priority)
	}
}

func TestFallbackAllEmpty(t *testing.T) {
	r := newRouter(emptyChan(), emptyChan(), emptyChan(), [3]int{3, 2, 1})

	start := time.Now()
	_, ok := r.Next(context.Background())
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected false when all queues empty")
	}
	if elapsed < 900*time.Millisecond || elapsed > 2*time.Second {
		t.Errorf("expected ~1s block, got %v", elapsed)
	}
}

func TestConcurrentSafe(t *testing.T) {
	const msgCount = 300
	msgs := make([]result, msgCount)
	for i := range msgs {
		msgs[i] = makeResult("high")
	}

	r := newRouter(filledChan(msgs), emptyChan(), emptyChan(), [3]int{3, 2, 1})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	results := make(chan result, msgCount)
	done := make(chan struct{})

	for range 10 {
		go func() {
			for {
				res, ok := r.Next(ctx)
				if !ok {
					done <- struct{}{}
					return
				}
				results <- res
			}
		}()
	}

	exited := 0
	collected := 0
	for exited < 10 {
		select {
		case <-results:
			collected++
		case <-done:
			exited++
		}
	}
	for {
		select {
		case <-results:
			collected++
		default:
			goto tally
		}
	}
tally:
	if collected != msgCount {
		t.Errorf("consumed %d messages, want %d", collected, msgCount)
	}
}

func TestContextCancel(t *testing.T) {
	r := newRouter(emptyChan(), emptyChan(), emptyChan(), [3]int{3, 2, 1})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	_, ok := r.Next(ctx)
	elapsed := time.Since(start)

	if ok {
		t.Fatal("expected false on cancelled context")
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("Next should return quickly on cancelled ctx, took %v", elapsed)
	}
}
