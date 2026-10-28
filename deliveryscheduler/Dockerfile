FROM golang:1.25-alpine AS builder
WORKDIR /app

RUN apk add --no-cache git

# Copy go.mod/go.sum files first (for layer caching)
COPY shared/go.mod shared/go.sum shared/
COPY api/go.mod api/go.sum api/
COPY processor/go.mod processor/go.sum processor/
COPY deliveryscheduler/go.mod deliveryscheduler/go.sum deliveryscheduler/

# Strip replace directives for CI/CD build (private modules not on GitHub)
RUN for m in shared api processor deliveryscheduler; do \
      sed -i '/^replace (/,/^)/d' $m/go.mod; \
    done

# Copy source code
COPY shared/ shared/
COPY api/ api/
COPY processor/ processor/
COPY deliveryscheduler/ deliveryscheduler/

RUN CGO_ENABLED=0 GOOS=linux go build -C deliveryscheduler -trimpath -o /deliveryscheduler-server ./cmd

FROM gcr.io/distroless/static-debian12
COPY --from=builder /deliveryscheduler-server /deliveryscheduler-server
ENTRYPOINT ["/deliveryscheduler-server"]
