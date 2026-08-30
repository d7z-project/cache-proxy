.PHONY: test test-fuzz test-race vet fmt tidy

FUZZ_TIME ?= 5s

cache-proxy: $(shell find . -type f -name "*.go" -not -path "./.git/*") go.mod go.sum
	@GOWORK=off CGO_ENABLED=0 go build -v -o $@

test:
	@GOWORK=off go test -timeout 60s ./...

test-fuzz:
	@GOWORK=off go test ./pkg/config -run='^$$' -fuzz='^FuzzConfigDecoders$$' -fuzztime=$(FUZZ_TIME)
	@GOWORK=off go test ./pkg/proxy/shared/httpcache -run='^$$' -fuzz='^FuzzMetadataRewriters$$' -fuzztime=$(FUZZ_TIME)
	@GOWORK=off go test ./pkg/proxy/oci -run='^$$' -fuzz='^FuzzOCIRequestParsers$$' -fuzztime=$(FUZZ_TIME)
	@GOWORK=off go test ./pkg/proxy/gomod -run='^$$' -fuzz='^FuzzParseModuleRequest$$' -fuzztime=$(FUZZ_TIME)
	@GOWORK=off go test ./pkg/repo/filerepo -run='^$$' -fuzz='^FuzzPathIndexBuilder$$' -fuzztime=$(FUZZ_TIME)
	@GOWORK=off go test ./pkg/proxy/deb -run='^$$' -fuzz='^FuzzDebianMetadataParsers$$' -fuzztime=$(FUZZ_TIME)
	@GOWORK=off go test ./pkg/proxy/rpm -run='^$$' -fuzz='^FuzzRPMMetadataParsers$$' -fuzztime=$(FUZZ_TIME)
	@GOWORK=off go test ./pkg/proxy/pacman -run='^$$' -fuzz='^FuzzParseDesc$$' -fuzztime=$(FUZZ_TIME)
	@GOWORK=off go test ./pkg/proxy/apk -run='^$$' -fuzz='^FuzzParseIndex$$' -fuzztime=$(FUZZ_TIME)

test-race:
	@GOWORK=off go test -timeout 5m -race ./...

vet:
	@GOWORK=off go vet ./...

fmt:
	@gofmt -w $(shell find . -type f -name "*.go" -not -path "./.git/*")

tidy:
	@GOWORK=off go mod tidy
