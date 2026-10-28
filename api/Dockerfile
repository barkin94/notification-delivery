FROM golang:1.25-alpine AS builder
WORKDIR /app

RUN apk add --no-cache git

COPY shared/go.mod shared/go.sum shared/
COPY api/go.mod api/go.sum api/
COPY processor/go.mod processor/go.sum processor/
COPY deliveryscheduler/go.mod deliveryscheduler/go.sum deliveryscheduler/

RUN go install github.com/swaggo/swag/cmd/swag@v1.16.6

COPY shared/ shared/
COPY api/ api/
COPY processor/ processor/
COPY deliveryscheduler/ deliveryscheduler/

RUN cd api && swag init -g cmd/main.go -o docs --parseDependency --parseInternal
RUN CGO_ENABLED=0 GOOS=linux go build -C api -trimpath -o /api-server ./cmd

FROM gcr.io/distroless/static-debian12
COPY --from=builder /api-server /api-server
EXPOSE 8080
ENTRYPOINT ["/api-server"]
