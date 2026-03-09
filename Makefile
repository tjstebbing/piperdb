# PiperDB Makefile

.PHONY: build test clean install lint fmt vet demo

# Default target
all: build test

# Build the CLI binary
build:
	@echo "🔨 Building piperdb..."
	@go build -o piperdb ./cmd/piperdb
	@echo "✅ Build complete"

# Run all tests
test:
	@echo "🧪 Running tests..."
	@go test ./...
	@echo "✅ Tests passed"

# Run tests with coverage
test-coverage:
	@echo "🧪 Running tests with coverage..."
	@go test -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "✅ Coverage report generated: coverage.html"

# Run benchmarks
benchmark:
	@echo "📊 Running benchmarks..."
	@go test -bench=. ./test/benchmarks/

# Run integration tests
test-integration:
	@echo "🔗 Running integration tests..."
	@go test -v ./test/integration/

# Run DSL tests
test-dsl:
	@echo "🔍 Running DSL tests..."
	@go test -v ./test/dsl/
	@go test -v ./internal/dsl/

# Install dependencies
install:
	@echo "📦 Installing dependencies..."
	@go mod download
	@go mod tidy

# Lint the code
lint:
	@echo "🔍 Running linter..."
	@go vet ./...
	@echo "✅ Linting complete"

# Format the code
fmt:
	@echo "📝 Formatting code..."
	@go fmt ./...
	@echo "✅ Formatting complete"

# Clean build artifacts
clean:
	@echo "🧹 Cleaning up..."
	@rm -f piperdb
	@rm -rf data/
	@rm -rf demo-data/
	@rm -f coverage.out coverage.html
	@echo "✅ Cleanup complete"

# Run the demo
demo: build
	@echo "🎬 Running demo..."
	@cd examples && ./demo.sh

# Development setup
dev-setup: install build test
	@echo "🚀 Development setup complete!"
	@echo ""
	@echo "Quick start commands:"
	@echo "  make build     - Build the binary"
	@echo "  make test      - Run tests"
	@echo "  make demo      - Run the demo"
	@echo "  make lint      - Check code quality"
	@echo ""

# Create release build
release:
	@echo "📦 Building release..."
	@GOOS=linux GOARCH=amd64 go build -ldflags "-w -s" -o piperdb-linux-amd64 ./cmd/piperdb
	@GOOS=darwin GOARCH=amd64 go build -ldflags "-w -s" -o piperdb-darwin-amd64 ./cmd/piperdb
	@GOOS=darwin GOARCH=arm64 go build -ldflags "-w -s" -o piperdb-darwin-arm64 ./cmd/piperdb
	@GOOS=windows GOARCH=amd64 go build -ldflags "-w -s" -o piperdb-windows-amd64.exe ./cmd/piperdb
	@echo "✅ Release binaries created"

# Check code quality
check: fmt lint test
	@echo "✅ All checks passed"

# Help target
help:
	@echo "PiperDB Makefile Commands:"
	@echo ""
	@echo "Development:"
	@echo "  make build          Build the CLI binary"
	@echo "  make test           Run all tests"
	@echo "  make test-coverage  Run tests with coverage report"
	@echo "  make benchmark      Run performance benchmarks"
	@echo "  make demo           Run the interactive demo"
	@echo "  make dev-setup      Complete development setup"
	@echo ""
	@echo "Code Quality:"
	@echo "  make fmt            Format code with go fmt"
	@echo "  make lint           Run go vet linter"
	@echo "  make check          Run fmt, lint, and test"
	@echo ""
	@echo "Testing:"
	@echo "  make test-integration  Run integration tests only"
	@echo "  make test-dsl         Run DSL tests only"
	@echo ""
	@echo "Utilities:"
	@echo "  make clean          Clean build artifacts"
	@echo "  make install        Download and tidy dependencies"
	@echo "  make release        Build release binaries"
	@echo "  make help           Show this help message"
