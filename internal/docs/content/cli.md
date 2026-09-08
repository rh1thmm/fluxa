# CLI reference

Run all commands from a workspace unless noted otherwise. A workspace is found
by walking upward from the current directory to `Fluxa.toml`.

## Workspace commands

`fluxa init <directory>` creates `Fluxa.toml`, `workflows/example.lua`, and a
`.gitignore` that excludes `.fluxa/` runtime state. It does not overwrite files.

`fluxa validate [workflow]` loads and validates the manifest and compiles each
selected Lua entry for syntax. `fluxa workflows` prints workflow names/entries.

## Executions

`fluxa run <workflow> [--input JSON]` starts a foreground execution. Object
input gains `trigger = "manual"`; arrays, strings, numbers, and booleans are
provided as `fluxa.input.data` with the same trigger marker.

`fluxa runs [workflow]` lists recent executions. `fluxa inspect <execution-id>`
prints execution state, result/error, tasks, and durable HTTP operations.

`fluxa retry <execution-id> [--force]` replays a failed, interrupted, or
ambiguous execution. `--force` is required when an operation has an ambiguous
external side effect; it is an explicit acknowledgement that duplication may be
possible.

## Runtime lifecycle

`fluxa daemon [--listen ADDR]` starts the local scheduler, queue dispatcher,
timer resumer, and webhook server. Default listener: `:8080`. It runs in the
foreground and stops on SIGINT/SIGTERM.

`fluxa activate <workflow>` and `fluxa deactivate <workflow>` persist trigger
activation state. Deactivation prevents schedule and webhook starts, but manual
`run` remains permitted. `fluxa schedules` shows configured cron expressions,
activation, and the persisted next run.

## Documentation

`fluxa docs <query>` searches embedded documents. Every query word must match a
document name or body. `fluxa docs host [--listen ADDR]` starts a read-only docs
server, default `:8081`. Browse `/`, `/docs/lua`, or `/docs/lua.md`.
