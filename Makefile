.PHONY: test test-race test-fuzz test-e2e vet fmt tidy

cache-proxy: $(shell find . -type f -name "*.go" -not -path "./.git/*") go.mod go.sum
	@CGO_ENABLED=0 go build -v -o $@

test:
	@go test -timeout 60s ./...

test-race:
	@go test -timeout 5m -race ./...

test-fuzz:
	@set -eu; \
	for file in $$(find . -type f -name '*_test.go' -exec grep -l '^func Fuzz' {} +); do \
		package=./$$(dirname "$$file"); \
		for target in $$(awk '/^func Fuzz/{name=$$2; sub(/\(.*/, "", name); print name}' "$$file"); do \
			go test -timeout 30s "$$package" -run='^$$' -fuzz="^$$target$$" -fuzztime=1000x; \
		done; \
	done

test-e2e:
	@./test/e2e/run.sh

vet:
	@go vet ./...

fmt:
	@gofmt -w $(shell find . -type f -name "*.go" -not -path "./.git/*")

tidy:
	@go mod tidy
