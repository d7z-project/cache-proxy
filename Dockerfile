FROM golang:1.25-alpine AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -v -o /out/cache-proxy .

FROM alpine:3.20
COPY --from=builder /out/cache-proxy /app/cache-proxy
VOLUME ["/data"]
ENTRYPOINT ["/app/cache-proxy"]
