cache-proxy: $(shell find . -type f -name "*.go" -not -path "./.git/*") go.mod go.sum
	@CGO_ENABLED=0 go build -v -o $@

test:
	@go test -timeout 60s ./...

fmt:
	@gofmt -w $(shell find . -type f -name "*.go" -not -path "./.git/*") && go mod tidy
