.PHONY: fmt lint test test-unit test-int build proto

fmt:
	go fmt ./...

lint: fmt
	@if [ -z "$$(which golangci-lint 2> /dev/null)" ]; then \
		echo "Installing golangci-lint..."; \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; \
	fi
	@GOPATH=$$(go env GOPATH); \
	if [ -z "$$GOPATH" ]; then GOPATH=$$HOME/go; fi; \
	$$GOPATH/bin/golangci-lint run

test-unit: fmt
	go test -v -short ./internal/...

test-int: fmt
	go test -v ./tests/...

test: test-unit test-int

build: fmt
	docker build -t ad-event-processor:latest .

proto:
	go run github.com/bufbuild/buf/cmd/buf@latest generate
