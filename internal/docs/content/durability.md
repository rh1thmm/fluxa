# Durability, replay, and queue semantics

Each workspace stores state in `.fluxa/state.db` using SQLite WAL and FULL
synchronous mode. Executions have IDs, workflow artifact versions, input,
status, result/error, task records, operation records, events, recovery
attempts, schedule state, waits, and queue records.

## Operation replay

On retry or queue recovery Fluxa executes the Lua chunk again. Stable task
identity plus operation ordinal and canonical HTTP request fingerprint locate
confirmed operations. Confirmed results are returned to Lua rather than sent a
second time. Pure transformations recompute normally.

An HTTP failure after dispatch can be `ambiguous`: the remote system may have
acted even though Fluxa did not receive a response. Fluxa pauses the execution
instead of claiming exactly-once delivery. An operator may use `retry --force`.

## Queue and crash behavior

Schedules and webhooks atomically create both an execution record and a queue
record before daemon dispatch. A daemon restart changes claimed queue records
back to queued. The worker reuses the same execution ID and replays it, so
completed operations are still protected.

There is one known current limitation: queue attempts do not yet have delayed
availability/backoff policy or cancellation. A workflow source/configuration
version mismatch fails replay rather than guessing with changed code.

## Schedules and waits

Schedule slots are uniquely claimed by workflow and timestamp. Inactive
workflows do not accept trigger starts. Waiting executions do not keep a Lua VM
or goroutine alive; the daemon scans durable due waits and resumes them.
