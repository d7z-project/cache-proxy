# ============================================
# Build stage for cache-proxy backend
# ============================================
FROM golang:1.25-alpine AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY web/dist ./web/dist
RUN CGO_ENABLED=0 go build -v \
    -ldflags "-X gopkg.d7z.net/cache-proxy/pkg/server.DefaultBackend=/data -X gopkg.d7z.net/cache-proxy/pkg/server.DefaultBind=0.0.0.0:8080" \
    -o cache-proxy .

# ============================================
# Minimal runtime image
# ============================================
FROM alpine:3.20
COPY --from=builder /src/cache-proxy /app/
ENV CACHE_PROXY_BACKEND=/data
ENV CACHE_PROXY_BIND=0.0.0.0:8080
EXPOSE 8080
VOLUME ["/data"]
ENTRYPOINT ["/app/cache-proxy"]
