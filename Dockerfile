# ============================================
# Multi-stage build for cache-proxy
# ============================================

# Stage 1: Build frontend
FROM node:20-alpine AS frontend
WORKDIR /src/web
COPY web/package*.json ./
RUN npm ci --silent
COPY web/ ./
RUN npm run build

# Stage 2: Build backend with embedded frontend
FROM golang:1.25-alpine AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . ./
COPY --from=frontend /src/web/dist ./web/dist
RUN CGO_ENABLED=0 go build -v \
    -ldflags "-X gopkg.d7z.net/cache-proxy/pkg/server.DefaultBackend=/data -X gopkg.d7z.net/cache-proxy/pkg/server.DefaultBind=0.0.0.0:8080" \
    -o cache-proxy .

# Stage 3: Minimal runtime image
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
ENV CACHE_PROXY_BACKEND=/data
ENV CACHE_PROXY_BIND=0.0.0.0:8080
RUN mkdir /data && chown -R nobody:nobody /data
USER nobody
COPY --from=builder /src/cache-proxy /app/
EXPOSE 8080
ENTRYPOINT ["/app/cache-proxy"]
