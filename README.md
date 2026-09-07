# Fluxa

Fluxa is a terminal-first, durable automation runtime for Lua.

> Make automation code beautiful. Make execution boring.

It keeps automation logic in normal source files while providing explicit task
boundaries, structured HTTP effects, SQLite-backed execution history, and
inspectable logs.

## Status

This is the initial v0.1 local-runtime release. It supports local workspaces,
manifest validation, foreground execution, explicit Lua tasks, HTTP, JSON,
durable run/task/operation records, and timeline inspection.

Scheduling/daemon deployment, retries and replay recovery, parallel collections,
and credential backends are intentionally not yet available. HTTP writes whose
outcome cannot be confirmed are recorded as `ambiguous` and stop the execution.

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
fluxa init outreach
cd outreach
fluxa validate
fluxa run outreach
fluxa runs
fluxa inspect <execution-id>
```

## Workflow format

`Fluxa.toml` describes the workflow lifecycle:

```toml
[workspace]
api_version = 1
default_environment = "development"
default_timezone = "UTC"

[workflows.outreach]
entry = "workflows/outreach.lua"
timeout = "15m"
secrets = []
```

The Lua file returns one entry function. Tasks execute sequentially and are
durable boundaries; ordinary Lua computation stays ordinary Lua.

```lua
return function(ctx)
  local response = task("fetch-leads", { timeout = "10s" }, function()
    return http.get("https://example.test/leads")
  end)

  local leads = json.decode(response.body)
  log.info("loaded leads", { count = #leads })
  return { processed = #leads }
end
```

Available v0.1 globals are `task`, `http`, `json`, `log`, `env`, and the entry
`ctx` table (`ctx.execution_id`). HTTP may be called only inside `task`.

## Commands

```text
fluxa init <directory>
fluxa validate [workflow]
fluxa workflows
fluxa run <workflow>
fluxa runs [workflow]
fluxa inspect <execution-id>
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

## License

MIT. See [LICENSE](LICENSE).
