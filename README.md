# Notification System

A notification delivery system built in Go. The API Service accepts notification requests, publishes them to NATS JetStream by priority, and the Processor Service picks them up for delivery and publishes results back to NATS JetStream. Retries use NATS JetStream's native NakWithDelay — no separate retry service is needed.

## Architecture

### Non-Scheduled Notification Delivery Flow

``` text
HTTP Client
     │
     ▼
API Service
│
├─── [ 1: Persist notification as <b>pending</b> ]
│
├─── [ 2: Dispatch NotificationReadyEvent to Topics By Priority]
│           │
│           ├──► <high_priority_topic>   ──┐
│           ├──► <normal_priority_topic> ──┼──► Processor Service
│           └──► <low_priority_topic>    ──┘    │
│                                               ├─── [ 3: Lock/Rate Limit Notifications ]
│                                               │
│                                               ├─── [ 4: Ntfn Delivery API (Mockoon server) ]
│                                               │
│                                               └─── [ 5: Dispatch NotificationDeliveryResultEvent ]       
|                                                            |
|                                                            └──► <status_update_topic> ──┐                   
│     ┌──────────────────────────────────────────────────────────────────────────────────┘
▼     ▼
└─── [ 6: Update notification status as <b>delivered</b>/<b>failed</b> ]
```

### Scheduled Notification Delivery Flow

```text
HTTP Client
     │
     ▼
API Service
│
├─── [ 1: Persist scheduled notification as <b>pending</b> with deliver_after ]
│
├─── [ 2: Publish NotificationsScheduledEvent (batch) ]
│           │
│           ▼
│    Delivery Scheduler Service
│           │
│           ├─── [ 3: Consume NotificationsScheduledEvent ]
│           ├─── [ 4: Persist schedule to Postgres ]
│           └─── [ 5: Poll for due notifications (atomic claim with FOR UPDATE SKIP LOCKED) ]
│                       │
│                       ▼
│               [ 6: Publish ScheduledNotificationDueEvent (batch IDs) ]
│                       │
│                       ▼
│    API Service (Consumer)
│           │
│           ├─── [ 7: Consume ScheduledNotificationDueEvent ]
│           ├─── [ 8: Hydrate full notification details from DB ]
│           └─── [ 9: Publish NotificationReadyEvent to Topics By Priority ]
│                    │
│                    ├──► <high_priority_topic>   ──┐
│                    ├──► <normal_priority_topic> ──┼──► Processor Service
│                    └──► <low_priority_topic>    ──┘    │
│                                                        ├─── [ 10: Lock/Rate Limit Notifications ]
│                                                        │
│                                                        ├─── [ 11: Ntfn Delivery API (Mockoon) ]
│                                                        │
│                                                        └─── [ 12: Dispatch NotificationDeliveryResultEvent ]       
│                                                                    |
│                                                                    └──► <status_update_topic> ──┐                   
│                                                                                                 │
│     ┌───────────────────────────────────────────────────────────────────────────────────────────┘
▼     ▼
└─── [ 13: Update notification status as <b>delivered</b>/<b>failed</b> ]
```

### Containers

| Service | Role |
|---------|------|
| [`api`](api/README.md) | HTTP API — create, list, cancel notifications; consumes scheduled-due events |
| [`processor`](processor/README.md) | Consumes delivery streams, delivers via webhook, publishes results |
| [`deliveryscheduler`](deliveryscheduler/README.md) | Consumes scheduled notifications, polls for due items, republishes when ready |
| `postgres` | Persistent store for notifications, delivery attempts, and scheduled deliveries |
| `nats` | NATS JetStream for async message passing |
| `otel-collector` | Receives OTLP traces, forwards to Tempo |
| `tempo` | Trace storage, queried by Grafana |
| `prometheus` | Scrapes `/metrics` from OTel Collector |
| `loki` | Logs storage, queried by Grafana |
| `grafana` | Dashboards — metrics (Prometheus) + traces (Tempo) + logs (Loki) |
| `mock-ntfn-provider` | Mockoon-based stub webhook endpoint for local delivery testing |
| `migrate-api` | One-shot container that runs `api` DB migrations on startup |
| `migrate-deliveryscheduler` | One-shot container that runs `deliveryscheduler` DB migrations on startup |

### Service Documentation

- [api/README.md](api/README.md) — HTTP API reference, request lifecycle, pagination, scheduled notifications, and delivery result consumer
- [processor/README.md](processor/README.md) — Priority router, delivery pipeline, retry mechanism, and rate limiting
- [deliveryscheduler/README.md](deliveryscheduler/README.md) — Scheduled delivery consumer, dispatcher, and atomic claim scheduling

## Prerequisites

- Docker and Docker Compose

Run `make help` to see all available commands.

## Running the system

### 1. Configure environment

Before running the services, `api/`, `processor/`, and `deliveryscheduler/` folders each need a `.env` file. Each folder contains an `.env.example` pre-filled for docker-compose — copy and rename it to `.env`.


### 2. Start all services

```bash
make up
```

Migrations run automatically as part of `make up` — dedicated `migrate-api` and `migrate-processor` containers run first and exit before the services start.

### 3. Verify

```bash
curl http://localhost:8080/api/v1/liveness
```

## API

Base URL: `http://localhost:8080`

**Swagger UI:** `http://localhost:8080/api/v1/docs/index.html`

## Observability

| URL | What you see |
|-----|-------------|
| `http://localhost:3000` | Grafana — metrics dashboard + traces + logs |
| `http://localhost:9090` | Prometheus — raw metrics |
| `http://localhost:3200` | Tempo — raw trace API |

Both services expose `/metrics` on their HTTP ports (`:8080` and `:8081`).

## Development

**Run tests**

```bash
make test
```

Integration tests spin up Postgres via testcontainers — Docker must be running.

**Lint**

```bash
make lint
```

**Regenerate Swagger docs** (only needed after changing handler signatures or adding endpoints)

```bash
make swag
```

**Run locally** (requires Postgres and NATS on localhost)

Start just the infrastructure, then run each service directly:

```bash
make infra

# in separate terminals
go run ./api/cmd
go run ./processor/cmd
```

Change `NATS_ADDR` and `DATABASE_URL` in the `.env` files to `localhost` for local runs. Migrations can be applied via the migrate container: `docker compose run --rm migrate-api`.

## Functional Requirements Status

### Notification Management API

- [x] Create notification requests with recipient, channel, content, and priority
- [x] Support batch creation (up to 1000 notifications per request)
- [x] Query notification status by ID or batch ID
- [x] Cancel pending notifications
- [x] List notifications with filtering (status, channel, date range) and pagination

### Processing Engine

- [x] Process notifications asynchronously via queue workers
- [x] Implement rate limiting: maximum 100 messages per second per channel
- [x] Priority queue support (high, normal, low)
- [x] Content validation (character limits, required fields)
- [x] Idempotency support to prevent duplicate sends

### Delivery & Retry Logic

- [x] Will require thinking and design by candidate

### Observability

- [x] Real-time metrics endpoint (OpenTelemetry exports metrics to Prometheus)
- [x] Structured logging with correlation IDs (OpenTelemetry injects trace ids to logs)
- [x] Health check endpoint

### Bonus Features

- [x] Failure Handling
- [x] Scheduled Notifications: Allow scheduling notifications for future delivery
- [ ] Template System: Support message templates with variable substitution
- [ ] WebSocket Updates: Real-time status updates via WebSocket
- [x] Distributed Tracing
- [x] GitHub Actions CI/CD: Automated testing and linting pipeline