
fmt:
	@(test -f "$(GOPATH)/bin/gofumpt" || go install golang.org/x/tools/cmd/goimports@latest) && \
	"$(GOPATH)/bin/gofumpt" -l -w .