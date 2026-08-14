# Contributing to YASE

Thanks for your interest in contributing! This guide covers how to set up a development environment, run the test suite, and submit a change.

## Development environment

- **Go 1.26+** (see `go.mod`)
- **Docker & Docker Compose** (for integration tests and local infra)

```bash
git clone https://github.com/efathom/yase.git
cd yase
go mod download
```

## Build & test

```bash
make build         # build core service binaries
make build-all     # build all binaries
make test-short    # unit tests only (no Docker required)
make test          # unit + integration tests (requires Docker)
make test-race     # race detector
make lint          # golangci-lint
make bench         # benchmarks
```

Run the full local stack with `make docker-infra` (Redis + Kafka) and the services described in [`docs/getting-started.md`](docs/getting-started.md).

## Code style

- Follow idiomatic Go conventions and run `gofmt` (CI enforces this).
- Use `log/slog` for structured logging, not `log.Printf`.
- Return errors up the call stack rather than panicking.
- Pass `context.Context` as the first argument to functions that do I/O.
- Prefer table-driven tests with `testify/assert`.

## Conventions

- Keep interfaces small and define them where they are used.
- Use the factory pattern for pluggable providers (embedder, reranker, connector, auth).
- New connectors self-register via `init()` in `connector.DefaultRegistry`.

## Pull request process

1. Open an issue (or comment on an existing one) to discuss larger changes before implementing.
2. Fork the repo and create a feature branch.
3. Make your change, adding tests where practical.
4. Run `go build ./...`, `go vet ./...`, `gofmt`, and `go test ./... -short -count=1`.
5. Open a PR using the pull request template.

All contributions are made under the [Apache 2.0](LICENSE) license.

## Getting help

- Ask questions in [Discussions](https://github.com/efathom/yase/discussions).
- Report bugs via [Issues](https://github.com/efathom/yase/issues).
