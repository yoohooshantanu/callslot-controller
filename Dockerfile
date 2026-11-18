# Multi-stage Dockerfile for ConcurrencyGovernor
FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /concurrency-governor ./cmd/server

FROM alpine:3.19
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app
COPY --from=builder /concurrency-governor /app/concurrency-governor

EXPOSE 8082
ENTRYPOINT ["/app/concurrency-governor"]
