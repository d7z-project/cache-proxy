
cache-proxy: $(shell find . -type f -name "*.go") go.mod go.sum
	@CGO_ENABLED=0 go build -v -o $@

fmt:
	@(test -f "$(GOPATH)/bin/gofumpt" || go install golang.org/x/tools/cmd/goimports@latest) && \
	"$(GOPATH)/bin/gofumpt" -l -w . && go mod tidy