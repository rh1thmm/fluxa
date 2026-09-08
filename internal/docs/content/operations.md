# Operations guide

## Local state

Workspace state is `.fluxa/state.db` plus SQLite WAL sidecars. Keep these files
together when backing up a workspace. Do not commit `.fluxa/`.

## Useful commands

```sh
fluxa validate
fluxa runs digest
fluxa inspect exec_abc
fluxa retry exec_abc
fluxa retry exec_abc --force
fluxa schedules
```

`inspect` shows tasks and HTTP operations. `retry` refuses source/version
mismatches and ambiguous effects unless forced.

## Daemon deployment today

Run `fluxa daemon --listen :8080` under your existing supervisor, for example
systemd, launchd, or a process manager. Ensure the daemon starts in the
workspace directory so it resolves that workspace manifest and state database.

## Safety checklist

1. Put external effects inside tasks.
2. Give loop-created tasks stable keys.
3. Use idempotency support from remote APIs where available.
4. Treat `paused_ambiguous` as an operator decision, not an automatic retry.
5. Back up state before deleting a workspace.

## Current operational limits

There is no built-in secret backend, service installer, remote deployment,
cancellation, retention/pruning, metrics, or daemon authentication yet. Do not
expose the webhook listener directly to an untrusted network without a reverse
proxy and application-level validation in the workflow.
