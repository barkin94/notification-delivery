FROM golang:1.25-alpine AS builder
WORKDIR /app

RUN apk add --no-cache git

COPY shared/go.mod shared/go.sum shared/
COPY api/go.mod api/go.sum api/
COPY processor/go.mod processor/go.sum processor/
COPY deliveryscheduler/go.mod deliveryscheduler/go.sum deliveryscheduler/

COPY shared/ shared/
COPY api/ api/
COPY processor/ processor/
COPY deliveryscheduler/ deliveryscheduler/

RUN CGO_ENABLED=0 GOOS=linux go build -C deliveryscheduler -trimpath -o /deliveryscheduler-server ./cmd

FROM gcr.io/distroless/static-debian12
COPY --from=builder /deliveryscheduler-server /deliveryscheduler-server
ENTRYPOINT ["/deliveryscheduler-server"]
