# NovaStor Code Style and Conventions

## Go Conventions

### Naming
- **MixedCaps** for Go identifiers, NOT `snake_case`
- Acronyms are all-caps: `CSI`, `NVMe`, `gRPC`, `HTTP`
- Example: `nvmeTargetClient` not `nvMeTargetClient`

### Error Handling
- Always wrap errors with context: `fmt.Errorf("operation failed: %w", err)`
- Use structured logging with `go.uber.org/zap`
- Log detailed errors internally, return generic errors externally

### Concurrency
- Use `context.Context` for cancellation
- Use `errgroup` for goroutine management
- Use channels for communication between goroutines

### Interfaces
- Define interfaces where they're **consumed**, not where they're **implemented**

### Testing
- Table-driven tests with `t.Run()` subtests
- Test files alongside source: `foo_test.go` next to `foo.go`
- Test BOTH success AND error paths
- All tests run with `-race` flag in CI

## Linting Rules
- **NEVER disable linting rules** via directive comments
- Fix the code, not the rules
- 16 linters enabled in golangci-lint
- Use `make check` before committing

## Commit Format
```
[Type] Short summary under 50 chars

Detailed explanation of what and why (not how).

Resolves #IssueNumber
```

**Types**: `[Fix]`, `[Feature]`, `[Refactor]`, `[Docs]`, `[Test]`, `[Chore]`

## Critical Rules
- **ALL commits are authored by Pascal Watteel only** - no AI attribution
- **NEVER create version tags** unless explicitly requested
- **NEVER hardcode secrets, passwords, or API keys**
- **NEVER use mock data or placeholder implementations**
