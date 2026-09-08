# Fluxa documentation

Fluxa is a local-first durable automation runtime. Workflows are ordinary Lua
files; explicit `task` calls mark durable work and Fluxa persists their external
operations in workspace SQLite state.

Read this documentation locally:

```sh
fluxa docs task
fluxa docs host --listen :8081
```

Documents:

- `cli` — commands, exit behavior, and operating the daemon.
- `manifest` — complete currently implemented `Fluxa.toml` schema.
- `lua` — workflow globals, tasks, HTTP, JSON, logging, and waits.
- `durability` — execution records, replay, queues, schedules, and safety.

The documentation is embedded in every Fluxa binary. `/docs/<name>.md` exposes
the source Markdown; `/docs/<name>` exposes a dependency-free browser view.

## Scope

The docs describe implemented behavior, not aspirations. Credential storage,
database/filesystem capabilities, concurrency helpers, workflow composition,
deployment, and service installation are deferred.
