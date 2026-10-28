package messaging_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"

	repopostgres "github.com/barkin94/notification-system/api/internal/db/postgres"
	"github.com/barkin94/notification-system/api/internal/service"
	"github.com/barkin94/notification-system/api/internal/transport/messaging"
	apipub "github.com/barkin94/notification-system/api/public"
	natsio "github.com/nats-io/nats.go"

	"github.com/barkin94/notification-system/shared/events"
	"github.com/barkin94/notification-system/shared/txmanager/buntx"
	natsmsg "github.com/barkin94/notification-system/shared/messaging/nats"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

var testDB *bun.DB

func TestMain(m *testing.M) {
	ctx := context.Background()

	pgContainer, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("testdb"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(120*time.Second),
		),
	)
	if err != nil {
		log.Fatalf("start postgres container: %v", err)
	}
	defer func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			log.Printf("terminate postgres container: %v", err)
		}
	}()

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Fatalf("get connection string: %v", err)
	}

	mig, err := migrate.New("file://../../../../api/migrations", "pgx5://"+connStr[len("postgres://"):])
	if err != nil {
		log.Fatalf("create migrator: %v", err)
	}
	if err := mig.Up(); err != nil && err != migrate.ErrNoChange {
		log.Fatalf("run migrations: %v", err)
	}

	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(connStr)))
	testDB = bun.NewDB(sqldb, pgdialect.New())
	defer testDB.Close() //nolint:errcheck

	os.Exit(m.Run())
}

func mustV7() uuid.UUID {
	id, err := uuid.NewV7()
	if err != nil {
		panic(err)
	}
	return id
}

func seedNotification(t *testing.T, id uuid.UUID, status string) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Millisecond)
	_, err := testDB.NewRaw(`
		INSERT INTO notifications
			(id, recipient, channel, content, priority, status, max_attempts, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, "+1", "sms", "hello", "normal", status, 4, now, now,
	).Exec(context.Background())
	if err != nil {
		t.Fatalf("seed notification: %v", err)
	}
}

func makeCloudEventResult(data any) natsmsg.Result[cloudevents.Event] {
	var ce cloudevents.Event
	switch e := data.(type) {
	case cloudevents.Event:
		ce = e
	default:
		ce = cloudevents.NewEvent()
		ce.SetID(uuid.New().String())
		ce.SetSource("test")
		ce.SetTime(time.Now().UTC())
		payload, _ := json.Marshal(data)
		ce.SetData(cloudevents.ApplicationJSON, payload)
	}

	return natsmsg.Result[cloudevents.Event]{
		Ctx:           context.Background(),
		Event:         ce,
		Msg:           &natsio.Msg{},
		AttemptNumber: 1,
	}
}

func makeDeliveryResultEvent(notifID, status string, attempt int, httpCode int, errMsg string, latency int) cloudevents.Event {
	ce := cloudevents.NewEvent()
	ce.SetID(uuid.New().String())
	ce.SetSource("notification-system-processor")
	ce.SetType(events.TypeNotificationDeliveryResult)
	ce.SetTime(time.Now().UTC())
	ce.SetData(cloudevents.ApplicationJSON, events.NotificationDeliveryResultData{
		NotificationID:  notifID,
		Status:          status,
		AttemptNumber:   attempt,
		HTTPStatusCode:  httpCode,
		ErrorMessage:    errMsg,
		LatencyMS:       latency,
	})
	return ce
}

func makeSvc() service.NotificationService {
	txManager := buntx.NewManager(testDB)
	return service.NewNotificationService(
		repopostgres.NewNotificationRepository(testDB),
		repopostgres.NewOutboxRepository(testDB),
		txManager,
	)
}

func runConsumer(svc service.NotificationService, result natsmsg.Result[cloudevents.Event]) {
	ch := make(chan natsmsg.Result[cloudevents.Event], 1)
	ch <- result
	close(ch)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	messaging.NewDeliveryResultConsumer(svc, ch).Run(ctx)
}

func TestDeliveryResultConsumer_delivered(t *testing.T) {
	notifID := mustV7()
	seedNotification(t, notifID, string(apipub.StatusPending))

	evt := makeDeliveryResultEvent(notifID.String(), string(apipub.StatusDelivered), 1, 200, "", 120)
	runConsumer(makeSvc(), makeCloudEventResult(evt))

	got, err := repopostgres.NewNotificationRepository(testDB).GetByID(context.Background(), notifID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Status != string(apipub.StatusDelivered) {
		t.Errorf("status = %q, want %q", got.Status, string(apipub.StatusDelivered))
	}
}

func TestDeliveryResultConsumer_failed(t *testing.T) {
	notifID := mustV7()
	seedNotification(t, notifID, string(apipub.StatusPending))

	evt := makeDeliveryResultEvent(notifID.String(), string(apipub.StatusFailed), 4, 0, "provider timeout", 500)
	runConsumer(makeSvc(), makeCloudEventResult(evt))

	got, err := repopostgres.NewNotificationRepository(testDB).GetByID(context.Background(), notifID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Status != string(apipub.StatusFailed) {
		t.Errorf("status = %q, want %q", got.Status, string(apipub.StatusFailed))
	}
}

func TestDeliveryResultConsumer_idempotent(t *testing.T) {
	notifID := mustV7()
	seedNotification(t, notifID, string(apipub.StatusPending))

	evt := makeDeliveryResultEvent(notifID.String(), string(apipub.StatusDelivered), 1, 200, "", 80)

	svc := makeSvc()
	runConsumer(svc, makeCloudEventResult(evt))
	runConsumer(svc, makeCloudEventResult(evt)) // second run: domain rejects delivered→delivered

	got, err := repopostgres.NewNotificationRepository(testDB).GetByID(context.Background(), notifID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Status != string(apipub.StatusDelivered) {
		t.Errorf("status = %q, want %q", got.Status, string(apipub.StatusDelivered))
	}
}