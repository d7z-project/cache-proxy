WEB_DIST := web/dist/cache-proxy-web/browser/index.html

cache-proxy: $(WEB_DIST) $(shell find . -type f -name "*.go") go.mod go.sum
	@CGO_ENABLED=0 go build -v -o $@

$(WEB_DIST): web/package.json web/package-lock.json $(shell find web/src -type f)
	@cd web && npm install && npm run build

web-build:
	@cd web && npm install && npm run build

test: $(WEB_DIST)
	@go test ./...

fmt:
	@gofmt -w $(shell find . -type f -name "*.go" -not -path "./web/dist/*") && go mod tidy
