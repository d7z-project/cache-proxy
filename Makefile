.PHONY: test test-race vet fmt tidy

cache-proxy: $(shell find . -type f -name "*.go" -not -path "./.git/*") go.mod go.sum
	@CGO_ENABLED=0 go build -v -o $@

test:
	@go test -timeout 60s ./...

test-race:
	@go test -timeout 5m -race ./...

vet:
	@go vet ./...

fmt:
	@gofmt -w $(shell find . -type f -name "*.go" -not -path "./.git/*")

tidy:
	@go mod tidy
