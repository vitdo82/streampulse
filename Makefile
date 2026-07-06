.PHONY: build run test lint clean install

APP_NAME    := streampulse
BUILD_DIR   := bin
CMD_DIR     := cmd/streampulse
VERSION     := $(shell git describe --tags --always --dirty 2>/dev/null || echo "v0.1.0-dev")
LDFLAGS     := -ldflags="-s -w -X main.version=$(VERSION)"

## build: Build the binary
build:
	@mkdir -p $(BUILD_DIR)
	go build $(LDFLAGS) -o $(BUILD_DIR)/$(APP_NAME) ./$(CMD_DIR)

## run: Build and run the TUI (uses mock data if no Kafka available)
#   make run ARGS="--brokers localhost:9093"
run: build
	$(BUILD_DIR)/$(APP_NAME) $(ARGS)

## serve: Build and run daemon mode
#   make serve ARGS="--brokers localhost:9093"
serve: build
	$(BUILD_DIR)/$(APP_NAME) serve $(ARGS)

## test: Run all tests
test:
	go test -v -race -count=1 ./...

## test-cover: Run tests with coverage
test-cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage: file://$(PWD)/coverage.html"

## lint: Run linter
lint:
	golangci-lint run ./...

## fmt: Format code
fmt:
	go fmt ./...
	goimports -w .

## tidy: Tidy module dependencies
tidy:
	go mod tidy

## deps: Install development tools
deps:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install golang.org/x/tools/cmd/goimports@latest

## install: Install to /usr/local/bin
install: build
	cp $(BUILD_DIR)/$(APP_NAME) /usr/local/bin/

## clean: Remove build artifacts
clean:
	rm -rf $(BUILD_DIR) coverage.out coverage.html

## release: Build release binaries for all platforms (requires goreleaser)
release:
	goreleaser build --snapshot --clean

## help: Show this help
help:
	@grep -E '^##' $(MAKEFILE_LIST) | sed -e 's/^## //' -e 's/:.*$$//' | column -t -s ':'
