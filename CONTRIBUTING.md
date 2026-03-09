# Contributing to PiperDB

Thank you for your interest in contributing to PiperDB! This document provides guidelines for contributing to the project.

## 🚀 Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/yourusername/piperdb.git
   cd piperdb
   ```
3. Build the project:
   ```bash
   go build ./cmd/piperdb
   ```
4. Run tests to ensure everything works:
   ```bash
   go test ./...
   ```

## 🛠️ Development Setup

### Prerequisites

- Go 1.21 or higher
- Git

### Building from Source

```bash
# Clone the repository
git clone https://github.com/tjstebbing/piperdb.git
cd piperdb

# Build the CLI tool
go build ./cmd/piperdb

# Run tests
go test ./...

# Run with coverage
go test -cover ./...
```

## 📝 Making Changes

### Code Style

- Follow standard Go formatting: `go fmt ./...`
- Run the linter: `go vet ./...`
- Write clear, descriptive commit messages
- Add tests for new functionality
- Update documentation when needed

### Project Structure

```
piperdb/
├── cmd/piperdb/         # CLI application
├── internal/            # Internal packages
│   ├── dsl/            # Query language
│   └── storage/        # Storage engine
├── pkg/                # Public API
│   ├── db/            # Main database interface
│   ├── types/         # Core types
│   └── config/        # Configuration
└── test/              # Tests and benchmarks
```

### Adding New Features

1. **DSL Features**: Add new syntax in `internal/dsl/`
   - Update lexer for new tokens
   - Add parser rules for new syntax
   - Implement execution in the executor
   - Add comprehensive tests

2. **Storage Features**: Modify `internal/storage/`
   - Ensure backward compatibility
   - Add migration code if needed
   - Test with existing data

3. **API Features**: Update `pkg/db/`
   - Maintain interface compatibility
   - Add comprehensive documentation
   - Include usage examples

## 🧪 Testing

### Running Tests

```bash
# Run all tests
go test ./...

# Run specific test suite
go test ./test/integration/ -v
go test ./test/dsl/ -v
go test ./internal/dsl/ -v

# Run with race detection
go test -race ./...

# Generate coverage report
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### Writing Tests

- Write unit tests for all public functions
- Add integration tests for end-to-end workflows
- Include benchmark tests for performance-critical code
- Test error conditions and edge cases

Example test:
```go
func TestQueryExecution(t *testing.T) {
    db := setupTestDB(t)
    defer db.Close()
    
    // Add test data
    err := db.AddItem(context.Background(), "test-list", map[string]interface{}{
        "name": "test item",
        "value": 42,
    })
    require.NoError(t, err)
    
    // Test query
    result, err := db.ExecutePipe(context.Background(), "test-list", "@value:>40", nil)
    assert.NoError(t, err)
    assert.Len(t, result.Items, 1)
}
```

## 📚 Documentation

### Code Documentation

- Document all public functions and types
- Include examples in GoDoc comments
- Explain complex algorithms and data structures

### README Updates

- Add new features to the examples section
- Update performance benchmarks if applicable
- Keep the feature list current

## 🐛 Reporting Bugs

When reporting bugs, please include:

1. **Description**: Clear description of the issue
2. **Steps to reproduce**: Minimal example that demonstrates the bug
3. **Expected behavior**: What you expected to happen
4. **Actual behavior**: What actually happened
5. **Environment**: Go version, OS, PiperDB version
6. **Logs**: Relevant error messages or logs

Example bug report:
```
## Bug Description
Query parsing fails for complex nested expressions

## Steps to Reproduce
1. Create a list with nested data
2. Run query: `@user.profile.name:john`
3. Parser returns error

## Expected Behavior
Should parse nested field access successfully

## Actual Behavior
Error: "unexpected token DOT"

## Environment
- Go: 1.21.5
- OS: macOS 14.2
- PiperDB: main branch (commit abc123)
```

## 💡 Feature Requests

For feature requests, please include:

1. **Use case**: Why you need this feature
2. **Proposed solution**: How you think it should work
3. **Alternatives**: Other ways to achieve the same goal
4. **DSL syntax**: If applicable, suggest query syntax

## 🔧 Pull Request Process

1. **Create a feature branch**: `git checkout -b feature/your-feature-name`
2. **Make your changes**: Follow the coding standards
3. **Add tests**: Ensure your code is well tested
4. **Update documentation**: Include relevant docs updates
5. **Test everything**: Run the full test suite
6. **Commit your changes**: Use clear commit messages
7. **Push to your fork**: `git push origin feature/your-feature-name`
8. **Create pull request**: Use the GitHub interface

### Pull Request Checklist

- [ ] Code follows the project style guidelines
- [ ] Self-review of the code completed
- [ ] Tests added for new functionality
- [ ] All tests pass locally
- [ ] Documentation updated (README, GoDoc, etc.)
- [ ] No breaking changes (or clearly documented)
- [ ] Commit messages are clear and descriptive

## 🎯 Good First Issues

New contributors can look for issues labeled:

- `good first issue` - Simple, well-defined tasks
- `help wanted` - Issues where maintainers would appreciate help
- `documentation` - Improvements to docs and examples
- `testing` - Adding or improving tests

## ❓ Questions

If you have questions about contributing:

1. Check existing issues and discussions
2. Create a new issue with the `question` label
3. Join community discussions (when available)

## 📄 License

By contributing to PiperDB, you agree that your contributions will be licensed under the MIT License.

## 🙏 Recognition

Contributors will be:

- Added to the contributors list
- Mentioned in release notes for significant contributions
- Credited in documentation for major features

Thank you for contributing to PiperDB! 🎉
