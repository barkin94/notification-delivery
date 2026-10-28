package delivery_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	apipub "github.com/barkin94/insider-notification/api/public"
	"github.com/barkin94/insider-notification/processor/internal/delivery"
	"github.com/barkin94/insider-notification/processor/internal/service"
	processorpub "github.com/barkin94/insider-notification/processor/public"
	"github.com/barkin94/insider-notification/shared/events"
	"github.com/barkin94/insider-notification/shared/messaging"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

func unmarshalCloudEvent(payload []byte) (cloudevents.Event, error) {
	var ce cloudevents.Event
	if err := json.Unmarshal(payload, &ce); err != nil {
		var zero cloudevents.Event
		return zero, err
	}
	return ce, nil
}

func extractDeliveryResult(payload any) (events.NotificationDeliveryResultData, error) {
	var data events.NotificationDeliveryResultData
	ce, ok := payload.(cloudevents.Event)
	if !ok {
		return data, errors.New("payload is not a CloudEvent")
	}
	return data, json.Unmarshal(ce.Data(), &data)
}

// --- fakes ---

type fakeOutboxRepo struct {
	mu      sync.Mutex
	saved   []publishedMsg
	errOn   string // return errTest when saving for this topic
}

type publishedMsg struct {
	topic   string
	payload any
}

func (f *fakeOutboxRepo) Save(_ context.Context, events ...messaging.OutboxEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range events {
		if f.errOn != "" && e.Topic == f.errOn {
			return errors.New("test error")
		}
		// Store the raw payload for calls() to unmarshal later
		f.saved = append(f.saved, publishedMsg{topic: e.Topic, payload: e.Payload})
	}
	return nil
}

func (f *fakeOutboxRepo) GetUnpublished(_ context.Context, _ int) ([]messaging.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) MarkPublished(_ context.Context, _ []string, _ time.Time) error {
	return nil
}

func (f *fakeOutboxRepo) IncrementRetryCount(_ context.Context, _ []string) error {
	return nil
}

func (f *fakeOutboxRepo) calls() []publishedMsg {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]publishedMsg, len(f.saved))
	copy(out, f.saved)
	// Unmarshal CloudEvent from payload for tests
	for i := range out {
		if b, ok := out[i].payload.([]byte); ok {
			if ce, err := unmarshalCloudEvent(b); err == nil {
				out[i].payload = ce
			}
		}
	}
	return out
}

func (f *fakeOutboxRepo) topicsPublished() []string {
	calls := f.calls()
	topics := make([]string, len(calls))
	for i, c := range calls {
		topics[i] = c.topic
	}
	return topics
}

type fakeLocker struct{ locked bool }

func (f *fakeLocker) TryLock(_ context.Context, _ string) (bool, error) { return f.locked, nil }
func (f *fakeLocker) Unlock(_ context.Context, _ string) error          { return nil }

type fakeDeliveryClient struct {
	result service.DeliveryResult
}

func (f *fakeDeliveryClient) Send(_ context.Context, to, channel, content string) service.DeliveryResult {
	return f.result
}

type fakeLimiter struct {
	allowed    bool
	retryAfter time.Duration
}

func (f *fakeLimiter) IsAllowed(_ context.Context, _ string) (bool, time.Duration, error) {
	return f.allowed, f.retryAfter, nil
}

func baseEvent() *testReadyEvent {
	return &testReadyEvent{
		id:           uuid.New().String(),
		channel:      string(apipub.ChannelEmail),
		recipient:    "test@example.com",
		content:      "test content",
		priority:     string(apipub.PriorityNormal),
		maxAttempts:  3,
		attemptNum:   1,
	}
}

func baseEventWithID() *testReadyEvent {
	return &testReadyEvent{
		id:           "62e89bde-ff46-4abd-b9ca-b0a77834d2f0",
		channel:      string(apipub.ChannelEmail),
		recipient:    "test@example.com",
		content:      "test content",
		priority:     string(apipub.PriorityNormal),
		maxAttempts:  3,
		attemptNum:   1,
	}
}

// testReadyEvent implements NotificationReadyEvent for testing
type testReadyEvent struct {
	id           string
	channel      string
	recipient    string
	content      string
	priority     string
	maxAttempts  int
	attemptNum   int
}

func (t *testReadyEvent) GetNotificationID() string { return t.id }
func (t *testReadyEvent) GetChannel() string       { return t.channel }
func (t *testReadyEvent) GetRecipient() string     { return t.recipient }
func (t *testReadyEvent) GetContent() string       { return t.content }
func (t *testReadyEvent) GetPriority() string      { return t.priority }
func (t *testReadyEvent) GetMaxAttempts() int      { return t.maxAttempts }

func runSingle(p *delivery.NotificationDeliveryPipeline, evt *testReadyEvent) error {
	ctx := context.Background()
	return p.Run(ctx, evt, evt.attemptNum)
}

func runWithCount(p *delivery.NotificationDeliveryPipeline, evt *testReadyEvent, attemptNum int) error {
	ctx := context.Background()
	evt.attemptNum = attemptNum
	return p.Run(ctx, evt, evt.attemptNum)
}

func isErrRetryAfter(err error) (delivery.ErrRetryAfter, bool) {
	var ra delivery.ErrRetryAfter
	ok := errors.As(err, &ra)
	return ra, ok
}

func newPipeline(repo *fakeOutboxRepo, dc service.NtfnDeliveryClient, lim service.Limiter, lockGranted bool) *delivery.NotificationDeliveryPipeline {
	m, _ := service.NewMetrics(nil)
	return delivery.NewNotificationDeliveryPipeline(
		repo,
		dc,
		lim,
		&fakeLocker{locked: lockGranted},
		m,
	)
}

// Lock miss: returns nil (Ack), no status published.
func TestPipeline_LockMiss(t *testing.T) {
	repo := &fakeOutboxRepo{}
	c := newPipeline(repo, nil, nil, false)

	if err := runSingle(c, baseEvent()); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if len(repo.calls()) != 0 {
		t.Errorf("expected no publishes, got %d", len(repo.calls()))
	}
}

// Full pipeline with terminal non-retryable failure: publishes failed status only.
func TestPipeline_TerminalFailure(t *testing.T) {
	repo := &fakeOutboxRepo{}
	dc := &fakeDeliveryClient{result: service.DeliveryResult{Success: false, Retryable: false}}
	lim := &fakeLimiter{allowed: true}
	c := newPipeline(repo, dc, lim, true)

	err := runSingle(c, baseEvent())
	if err != nil {
		t.Errorf("expected nil (Ack path), got %v", err)
	}
	calls := repo.calls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 publish, got %d: %v", len(calls), calls)
	}
	if calls[0].topic != processorpub.TopicStatus {
		t.Errorf("expected publish to %s, got %s", processorpub.TopicStatus, calls[0].topic)
	}
	evt, err := extractDeliveryResult(calls[0].payload)
	if err != nil {
		t.Fatalf("failed to extract delivery result: %v", err)
	}
	if evt.Status != string(apipub.StatusFailed) {
		t.Errorf("expected status %s, got %s", string(apipub.StatusFailed), evt.Status)
	}
}

// Success result: one status event with status=delivered, no retry.
func TestPipeline_Delivered(t *testing.T) {
	repo := &fakeOutboxRepo{}
	dc := &fakeDeliveryClient{result: service.DeliveryResult{Success: true, StatusCode: 202, LatencyMS: 50}}
	lim := &fakeLimiter{allowed: true}
	c := newPipeline(repo, dc, lim, true)

	if err := runSingle(c, baseEvent()); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	calls := repo.calls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 publish (delivered), got %d", len(calls))
	}
	evt, err := extractDeliveryResult(calls[0].payload)
	if err != nil {
		t.Fatalf("failed to extract delivery result: %v", err)
	}
	if evt.Status != string(apipub.StatusDelivered) {
		t.Errorf("expected status %s, got %s", string(apipub.StatusDelivered), evt.Status)
	}
	if evt.HTTPStatusCode != 202 {
		t.Errorf("expected status code 202, got %d", evt.HTTPStatusCode)
	}
}

// Retryable failure with attempts remaining: returns ErrRetryAfter (NakWithDelay path).
func TestPipeline_Retryable_ReturnsErrRetryAfter(t *testing.T) {
	repo := &fakeOutboxRepo{}
	dc := &fakeDeliveryClient{result: service.DeliveryResult{Success: false, Retryable: true, StatusCode: 503}}
	lim := &fakeLimiter{allowed: true}
	c := newPipeline(repo, dc, lim, true)

	err := runSingle(c, baseEventWithID())

	ra, ok := isErrRetryAfter(err)
	if !ok {
		t.Fatalf("expected ErrRetryAfter, got %v", err)
	}
	if ra.Delay <= 0 {
		t.Errorf("expected positive delay, got %s", ra.Delay)
	}
	if len(repo.calls()) != 0 {
		t.Errorf("expected no publishes on retry path, got %v", repo.topicsPublished())
	}
}

// Retryable failure at max attempts: returns nil (Ack), publishes status=failed.
func TestPipeline_Exhausted_Failed(t *testing.T) {
	repo := &fakeOutboxRepo{}
	dc := &fakeDeliveryClient{result: service.DeliveryResult{Success: false, Retryable: true, StatusCode: 503}}
	lim := &fakeLimiter{allowed: true}
	c := newPipeline(repo, dc, lim, true)

	// attemptNumber=4 = MaxAttempts → terminal
	evt := baseEventWithID()
	evt.maxAttempts = 4
	err := runWithCount(c, evt, 4)

	if err != nil {
		t.Errorf("expected nil (Ack path), got %v", err)
	}
	calls := repo.calls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 publish (failed), got %d", len(calls))
	}
	if calls[0].topic != processorpub.TopicStatus {
		t.Errorf("expected publish to %s, got %s", processorpub.TopicStatus, calls[0].topic)
	}
	last, err := extractDeliveryResult(calls[0].payload)
	if err != nil {
		t.Fatalf("failed to extract delivery result: %v", err)
	}
	if last.Status != string(apipub.StatusFailed) {
		t.Errorf("expected status %s, got %s", string(apipub.StatusFailed), last.Status)
	}
	if last.AttemptNumber != 4 {
		t.Errorf("expected AttemptNumber 4, got %d", last.AttemptNumber)
	}
}

// Non-retryable failure: returns nil (Ack), publishes status=failed.
func TestPipeline_NonRetryable_Failed(t *testing.T) {
	repo := &fakeOutboxRepo{}
	dc := &fakeDeliveryClient{result: service.DeliveryResult{Success: false, Retryable: false, StatusCode: 400}}
	lim := &fakeLimiter{allowed: true}
	c := newPipeline(repo, dc, lim, true)

	if err := runSingle(c, baseEvent()); err != nil {
		t.Errorf("expected nil (Ack path), got %v", err)
	}
	calls := repo.calls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 publish (failed), got %d", len(calls))
	}
	last, err := extractDeliveryResult(calls[0].payload)
	if err != nil {
		t.Fatalf("failed to extract delivery result: %v", err)
	}
	if last.Status != string(apipub.StatusFailed) {
		t.Errorf("expected status %s, got %s", string(apipub.StatusFailed), last.Status)
	}
}

// Rate limited: returns ErrRetryAfter, no delivery attempted, no status published.
func TestPipeline_RateLimited_ReturnsErrRetryAfter(t *testing.T) {
	repo := &fakeOutboxRepo{}
	lim := &fakeLimiter{allowed: false, retryAfter: 2 * time.Second}
	c := newPipeline(repo, nil, lim, true)

	err := runSingle(c, baseEvent())

	ra, ok := isErrRetryAfter(err)
	if !ok {
		t.Fatalf("expected ErrRetryAfter, got %v", err)
	}
	if ra.Delay != 2*time.Second {
		t.Errorf("expected delay 2s, got %s", ra.Delay)
	}
	if len(repo.calls()) != 0 {
		t.Errorf("expected no publishes on rate-limit path, got %v", repo.topicsPublished())
	}
}

// Rate limit with zero retryAfter falls back to 1s minimum.
func TestPipeline_RateLimited_ZeroRetryAfter_DefaultsToOneSecond(t *testing.T) {
	repo := &fakeOutboxRepo{}
	lim := &fakeLimiter{allowed: false, retryAfter: 0}
	c := newPipeline(repo, nil, lim, true)

	err := runSingle(c, baseEvent())

	ra, ok := isErrRetryAfter(err)
	if !ok {
		t.Fatalf("expected ErrRetryAfter, got %v", err)
	}
	if ra.Delay != time.Second {
		t.Errorf("expected 1s fallback delay, got %s", ra.Delay)
	}
}

// --- delivery attempt number tests ---

// AttemptNumber in status event equals attemptNumber.
func TestPipeline_AttemptNumber_FromNATSMetadata(t *testing.T) {
	repo := &fakeOutboxRepo{}
	dc := &fakeDeliveryClient{result: service.DeliveryResult{Success: true}}
	lim := &fakeLimiter{allowed: true}
	c := newPipeline(repo, dc, lim, true)

	err := runWithCount(c, baseEventWithID(), 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	calls := repo.calls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 publish, got %d", len(calls))
	}
	evt, err := extractDeliveryResult(calls[0].payload)
	if err != nil {
		t.Fatalf("failed to extract delivery result: %v", err)
	}
	if evt.AttemptNumber != 3 {
		t.Errorf("AttemptNumber = %d, want 3", evt.AttemptNumber)
	}
}

// Retryable failure: ErrRetryAfter delay is positive (backoff formula applied).
func TestPipeline_Retryable_DelayIsPositive(t *testing.T) {
	repo := &fakeOutboxRepo{}
	dc := &fakeDeliveryClient{result: service.DeliveryResult{Success: false, Retryable: true}}
	lim := &fakeLimiter{allowed: true}
	c := newPipeline(repo, dc, lim, true)

	err := runWithCount(c, baseEventWithID(), 2)
	ra, ok := isErrRetryAfter(err)
	if !ok {
		t.Fatalf("expected ErrRetryAfter, got %v", err)
	}
	if ra.Delay <= 0 {
		t.Errorf("expected positive delay, got %s", ra.Delay)
	}
}

// Status publish error causes a non-nil return so caller Nacks.
func TestPipeline_StatusPublishError_ReturnsError(t *testing.T) {
	repo := &fakeOutboxRepo{errOn: processorpub.TopicStatus}
	dc := &fakeDeliveryClient{result: service.DeliveryResult{Success: true}}
	lim := &fakeLimiter{allowed: true}
	c := newPipeline(repo, dc, lim, true)

	err := runSingle(c, baseEventWithID())
	if err == nil {
		t.Fatal("expected error from status publish")
	}
}