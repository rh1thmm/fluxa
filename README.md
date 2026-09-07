# Fluxa

Fluxa is a terminal-first, durable automation runtime for Lua.

> Make automation code beautiful. Make execution boring.

It keeps automation logic in normal source files while providing explicit task
boundaries, structured HTTP effects, SQLite-backed execution history, and
inspectable logs.

## Status

This is the initial v0.1 local-runtime release. It supports local workspaces,
manifest validation, foreground execution, explicit Lua tasks, HTTP, JSON,
durable run/task/operation records, timeline inspection, and conservative
execution replay.

Scheduling/daemon deployment, parallel collections, and credential backends are
intentionally not yet available. HTTP writes whose outcome cannot be confirmed
are recorded as `ambiguous` and stop the execution until an explicit forced retry.

## Install

Requirements: Go 1.22 or newer and GNU Make.

```sh
git clone https://github.com/rh1thmm/fluxa.git
cd fluxa
./scripts/install.sh
```

The installer builds `bin/fluxa` and copies it to `~/.local/bin/fluxa` by
default. Set `FLUXA_BIN_DIR` to choose another destination.

```sh
./scripts/update.sh          # pull, build, and reinstall
./scripts/update.sh --no-pull
./scripts/delete.sh          # interactive permanent removal
```

`delete.sh` removes Fluxa’s installed binary, global application data, and
workspaces registered by `fluxa init`. It does not scan your home directory for
unregistered projects.

## Quick start

```sh
fluxa init hello-fluxa
cd hello-fluxa
fluxa validate
fluxa run example
fluxa runs
fluxa inspect <execution-id>
fluxa retry <execution-id>
```

## Workflow format

`Fluxa.toml` describes the workflow lifecycle:

```toml
[workspace]
api_version = 1
default_timezone = "UTC"

[workflows.example]
entry = "workflows/example.lua"
timeout = "30s"
```

The Lua file itself is the workflow. Its top-level return value is the workflow
result. Tasks execute sequentially and are durable boundaries; ordinary Lua
computation stays ordinary Lua.

```lua
log.info("Starting outreach", { execution_id = fluxa.execution_id })

local response = task("fetch-leads")
  :retry(3)
  :timeout("10s")
  :run(function()
    return http.get("https://example.test/leads")
  end)

local leads = json.decode(response.body)
log.info("loaded leads", { count = #leads })

return { processed = #leads }
```

Available v0.1 globals are `task`, `http`, `json`, `log`, `env`, and `fluxa`.
`fluxa.execution_id`, `fluxa.workflow`, `fluxa.input`, and `fluxa.attempt` are
available when needed. HTTP may be called only inside `task`.

## Commands

```text
fluxa init <directory>
fluxa validate [workflow]
fluxa workflows
fluxa run <workflow>
fluxa runs [workflow]
fluxa inspect <execution-id>
fluxa retry <execution-id> [--force]
fluxa version
```

## Development

```sh
make build       # bin/fluxa
make test
make test-race
make vet
make lint
make dist
```

The release build is CGO-free and `make dist` produces Linux amd64/arm64,
macOS amd64/arm64, and Windows amd64 binaries.

## Data and safety

Each workspace stores runtime state under `.fluxa/state.db` using SQLite WAL.
Do not separate a SQLite WAL database from its `-wal` and `-shm` sidecar files.
Lua workspaces are trusted local code in this release; Fluxa restricts exposed
capabilities but does not claim hostile-code sandboxing.

### Replay guarantees and constraints

`fluxa retry` resumes the original execution lineage. It replays Lua code, but
returns persisted results for confirmed HTTP operations with the same stable task
identity, ordinal, and canonical request fingerprint. Confirmed side effects are
not sent again.

Use `key` for dynamic task instances:

```lua
task("write-email", { key = lead.id }, function() ... end)
```

Unkeyed tasks use deterministic invocation order and will fail replay if that
order changes. A retry refuses when workspace source/configuration changes, when
task identity diverges, or when a request fingerprint changes. An ambiguous
unsafe HTTP operation requires `fluxa retry <id> --force`; Fluxa does not claim
exactly-once delivery.

Set `idempotency = true` on generic HTTP requests when an endpoint honors the
standard `Idempotency-Key` header. Fluxa derives a stable key per operation and
keeps it stable across attempts.

## License

MIT. See [LICENSE](LICENSE).
