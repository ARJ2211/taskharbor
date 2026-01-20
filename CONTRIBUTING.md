# Contributing

Thanks for contributing to TaskHarbor.

## Development setup
- Go 1.22+ recommended
- Run tests: go test ./...
- Run with race detector when changing concurrency: go test -race ./...

## Style
- gofmt is required
- Keep the public API small and stable
- Prefer adding behavior behind options/middleware instead of expanding exported types

## Pull requests
- Include tests for new behavior
- Update docs if semantics change
- Keep changes focused (one feature per PR)

## Reporting issues
When filing an issue, include:
- Go version
- Driver (memory/postgres)
- Expected behavior vs actual behavior
- Minimal repro if possible
