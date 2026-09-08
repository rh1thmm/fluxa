// Package store owns Fluxa's durable execution ledger.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

const (
	ExecutionRunning         = "running"
	ExecutionCompleted       = "completed"
	ExecutionFailed          = "failed"
	ExecutionInterrupted     = "interrupted"
	ExecutionWaiting         = "waiting"
	ExecutionQueued          = "queued"
	ExecutionPausedAmbiguous = "paused_ambiguous"
	OperationPending         = "pending"
	OperationRunning         = "running"
	OperationCompleted       = "completed"
	OperationFailed          = "failed"
	OperationAmbiguous       = "ambiguous"
	OperationCancelled       = "cancelled"
)

type Store struct{ db *sql.DB }
type Execution struct {
	ID, Workflow, Version, Status   string
	Input, ArtifactManifest, Result json.RawMessage
	RecoveryCount                   int
	StartedAt                       time.Time
	EndedAt                         *time.Time
	Error                           string
}
type Task struct {
	ID, ExecutionID, InstanceKey, ParentInstanceKey, Name, TaskKey, IdentityMode, Status string
	InvocationOrdinal                                                                    int
	Attempt                                                                              int
	Result                                                                               json.RawMessage
	Error                                                                                string
}
type Operation struct {
	ID, TaskID, Fingerprint, Capability, Status, IdempotencyKey string
	Ordinal, Attempt                                            int
	Descriptor, Result                                          json.RawMessage
	Error                                                       string
}
type Wait struct {
	ExecutionID string
	Ordinal     int
	Until       time.Time
	Status      string
}
type QueuedExecution struct {
	ExecutionID, Workflow string
	Attempt               int
}

func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, err
	}
	if _, err = db.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;`); err != nil {
		_ = db.Close()
		return nil, err
	}
	s := &Store{db: db}
	if err := s.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}
func (s *Store) Close() error { return s.db.Close() }

type migration struct {
	version    int
	statements []string
}

var migrations = []migration{
	{1, []string{
		`CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS executions (id TEXT PRIMARY KEY, workflow TEXT NOT NULL, version TEXT NOT NULL, status TEXT NOT NULL, input_json BLOB, error TEXT, started_at TEXT NOT NULL, ended_at TEXT)`,
		`CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, execution_id TEXT NOT NULL REFERENCES executions(id), name TEXT NOT NULL, task_key TEXT, status TEXT NOT NULL, attempt INTEGER NOT NULL, error TEXT, created_at TEXT NOT NULL, ended_at TEXT)`,
		`CREATE TABLE IF NOT EXISTS operations (id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(id), ordinal INTEGER NOT NULL, fingerprint TEXT NOT NULL, capability TEXT NOT NULL, status TEXT NOT NULL, attempt INTEGER NOT NULL, result_json BLOB, error TEXT, created_at TEXT NOT NULL, ended_at TEXT, UNIQUE(task_id, ordinal))`,
		`CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, execution_id TEXT NOT NULL REFERENCES executions(id), task_id TEXT, operation_id TEXT, type TEXT NOT NULL, data_json BLOB, created_at TEXT NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS executions_workflow_started ON executions(workflow, started_at DESC)`,
		`CREATE INDEX IF NOT EXISTS tasks_execution ON tasks(execution_id)`,
		`CREATE INDEX IF NOT EXISTS operations_task ON operations(task_id)`,
	}},
	{2, []string{
		`ALTER TABLE executions ADD COLUMN artifact_manifest_json BLOB`,
		`ALTER TABLE executions ADD COLUMN recovery_count INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE tasks ADD COLUMN instance_key TEXT`,
		`ALTER TABLE tasks ADD COLUMN parent_instance_key TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE tasks ADD COLUMN invocation_ordinal INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE tasks ADD COLUMN identity_mode TEXT NOT NULL DEFAULT 'legacy'`,
		`ALTER TABLE tasks ADD COLUMN result_json BLOB`,
		`ALTER TABLE operations ADD COLUMN descriptor_json BLOB`,
		`ALTER TABLE operations ADD COLUMN idempotency_key TEXT`,
		`CREATE UNIQUE INDEX IF NOT EXISTS tasks_execution_instance_key ON tasks(execution_id, instance_key) WHERE instance_key IS NOT NULL`,
		`CREATE TABLE IF NOT EXISTS operation_attempts (id INTEGER PRIMARY KEY AUTOINCREMENT, operation_id TEXT NOT NULL REFERENCES operations(id), attempt INTEGER NOT NULL, kind TEXT NOT NULL, status TEXT NOT NULL, error_kind TEXT, error TEXT, started_at TEXT NOT NULL, ended_at TEXT, UNIQUE(operation_id, attempt))`,
		`CREATE TABLE IF NOT EXISTS recovery_attempts (id INTEGER PRIMARY KEY AUTOINCREMENT, execution_id TEXT NOT NULL REFERENCES executions(id), attempt INTEGER NOT NULL, source_version TEXT NOT NULL, force_ambiguous INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, error TEXT, started_at TEXT NOT NULL, ended_at TEXT, UNIQUE(execution_id, attempt))`,
	}},
	{3, []string{
		`ALTER TABLE executions ADD COLUMN result_json BLOB`,
	}},
	{4, []string{
		`CREATE TABLE IF NOT EXISTS workflow_activation (workflow TEXT PRIMARY KEY, active INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS schedule_state (workflow TEXT PRIMARY KEY, next_run_at TEXT, updated_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS schedule_runs (workflow TEXT NOT NULL, scheduled_at TEXT NOT NULL, execution_id TEXT, created_at TEXT NOT NULL, PRIMARY KEY(workflow, scheduled_at))`,
	}},
	{5, []string{
		`CREATE TABLE IF NOT EXISTS execution_waits (execution_id TEXT NOT NULL REFERENCES executions(id), ordinal INTEGER NOT NULL, until_at TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL, completed_at TEXT, PRIMARY KEY(execution_id, ordinal))`,
		`CREATE INDEX IF NOT EXISTS execution_waits_due ON execution_waits(status, until_at)`,
	}},
	{6, []string{
		`CREATE TABLE IF NOT EXISTS execution_queue (execution_id TEXT PRIMARY KEY REFERENCES executions(id), workflow TEXT NOT NULL, status TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 0, available_at TEXT NOT NULL, claimed_at TEXT, finished_at TEXT, created_at TEXT NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS execution_queue_ready ON execution_queue(status, available_at, created_at)`,
	}},
}

func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)`); err != nil {
		return err
	}
	for _, m := range migrations {
		var exists int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE version=?`, m.version).Scan(&exists); err != nil {
			return err
		}
		if exists != 0 {
			continue
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		for _, stmt := range m.statements {
			if _, err = tx.ExecContext(ctx, stmt); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("migration %d: %w", m.version, err)
			}
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO schema_migrations(version, applied_at) VALUES(?,?)`, m.version, now()); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
func now() string          { return time.Now().UTC().Format(time.RFC3339Nano) }
func marshal(v any) []byte { b, _ := json.Marshal(v); return b }
func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func (s *Store) CreateExecution(ctx context.Context, e Execution, input, manifest any) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO executions(id,workflow,version,status,input_json,artifact_manifest_json,started_at) VALUES(?,?,?,?,?,?,?)`, e.ID, e.Workflow, e.Version, e.Status, marshal(input), marshal(manifest), now())
	return err
}

// EnqueueExecution durably creates both the execution ledger row and its local
// dispatch record. A daemon crash cannot leave one without the other.
func (s *Store) EnqueueExecution(ctx context.Context, e Execution, input, manifest any) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO executions(id,workflow,version,status,input_json,artifact_manifest_json,started_at) VALUES(?,?,?,?,?,?,?)`, e.ID, e.Workflow, e.Version, ExecutionQueued, marshal(input), marshal(manifest), now()); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO execution_queue(execution_id,workflow,status,available_at,created_at) VALUES(?,?,?,?,?)`, e.ID, e.Workflow, ExecutionQueued, now(), now()); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO events(execution_id,type,data_json,created_at) VALUES(?,?,?,?)`, e.ID, "execution.queued", marshal(map[string]any{"workflow": e.Workflow, "version": e.Version}), now()); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
func (s *Store) RecoverQueue(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE execution_queue SET status=?,claimed_at=NULL WHERE status=?`, ExecutionQueued, ExecutionRunning); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE executions SET status=? WHERE id IN (SELECT execution_id FROM execution_queue WHERE status=?) AND status=?`, ExecutionQueued, ExecutionQueued, ExecutionRunning); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
func (s *Store) ClaimQueuedExecution(ctx context.Context) (QueuedExecution, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return QueuedExecution{}, err
	}
	var q QueuedExecution
	err = tx.QueryRowContext(ctx, `SELECT execution_id,workflow,attempt FROM execution_queue WHERE status=? AND available_at<=? ORDER BY created_at LIMIT 1`, ExecutionQueued, now()).Scan(&q.ExecutionID, &q.Workflow, &q.Attempt)
	if err == sql.ErrNoRows {
		_ = tx.Rollback()
		return q, err
	}
	if err != nil {
		_ = tx.Rollback()
		return q, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE execution_queue SET status=?,attempt=attempt+1,claimed_at=? WHERE execution_id=? AND status=?`, ExecutionRunning, now(), q.ExecutionID, ExecutionQueued); err != nil {
		_ = tx.Rollback()
		return q, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE executions SET status=? WHERE id=?`, ExecutionRunning, q.ExecutionID); err != nil {
		_ = tx.Rollback()
		return q, err
	}
	q.Attempt++
	return q, tx.Commit()
}
func (s *Store) FinishQueuedExecution(ctx context.Context, executionID string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE execution_queue SET status='finished',finished_at=? WHERE execution_id=?`, now(), executionID)
	return err
}
func (s *Store) ReleaseQueuedExecution(ctx context.Context, executionID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE execution_queue SET status=?,claimed_at=NULL WHERE execution_id=? AND status=?`, ExecutionQueued, executionID, ExecutionRunning); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE executions SET status=? WHERE id=? AND status=?`, ExecutionQueued, executionID, ExecutionRunning); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
func (s *Store) FinishExecution(ctx context.Context, id, status, msg string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE executions SET status=?, error=?, ended_at=? WHERE id=?`, status, msg, now(), id)
	return err
}
func (s *Store) SaveExecutionResult(ctx context.Context, id string, result any) error {
	_, err := s.db.ExecContext(ctx, `UPDATE executions SET result_json=? WHERE id=?`, marshal(result), id)
	return err
}
func (s *Store) SetExecutionWaiting(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE executions SET status=?,error='',ended_at=NULL WHERE id=?`, ExecutionWaiting, id)
	return err
}
func (s *Store) CreateWait(ctx context.Context, wait Wait) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO execution_waits(execution_id,ordinal,until_at,status,created_at) VALUES(?,?,?,?,?)`, wait.ExecutionID, wait.Ordinal, wait.Until.UTC().Format(time.RFC3339Nano), "waiting", now())
	return err
}
func (s *Store) Wait(ctx context.Context, executionID string, ordinal int) (Wait, error) {
	var wait Wait
	var until string
	err := s.db.QueryRowContext(ctx, `SELECT execution_id,ordinal,until_at,status FROM execution_waits WHERE execution_id=? AND ordinal=?`, executionID, ordinal).Scan(&wait.ExecutionID, &wait.Ordinal, &until, &wait.Status)
	if err != nil {
		return wait, err
	}
	wait.Until, err = time.Parse(time.RFC3339Nano, until)
	return wait, err
}
func (s *Store) CompleteWait(ctx context.Context, executionID string, ordinal int) error {
	_, err := s.db.ExecContext(ctx, `UPDATE execution_waits SET status='completed',completed_at=? WHERE execution_id=? AND ordinal=?`, now(), executionID, ordinal)
	return err
}
func (s *Store) DueWaitingExecutions(ctx context.Context, at time.Time) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT e.id FROM executions e JOIN execution_waits w ON w.execution_id=e.id WHERE e.status=? AND w.status='waiting' AND w.until_at<=?`, ExecutionWaiting, at.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
func (s *Store) SetWorkflowActive(ctx context.Context, workflow string, active bool) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO workflow_activation(workflow,active,updated_at) VALUES(?,?,?) ON CONFLICT(workflow) DO UPDATE SET active=excluded.active,updated_at=excluded.updated_at`, workflow, boolInt(active), now())
	return err
}
func (s *Store) WorkflowActive(ctx context.Context, workflow string) (bool, error) {
	var active int
	err := s.db.QueryRowContext(ctx, `SELECT active FROM workflow_activation WHERE workflow=?`, workflow).Scan(&active)
	if err == sql.ErrNoRows {
		return true, nil
	}
	return active != 0, err
}
func (s *Store) ScheduleNextRun(ctx context.Context, workflow string) (*time.Time, error) {
	var raw sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT next_run_at FROM schedule_state WHERE workflow=?`, workflow).Scan(&raw)
	if err == sql.ErrNoRows || !raw.Valid {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	v, err := time.Parse(time.RFC3339Nano, raw.String)
	if err != nil {
		return nil, err
	}
	return &v, nil
}
func (s *Store) SetScheduleNextRun(ctx context.Context, workflow string, next time.Time) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO schedule_state(workflow,next_run_at,updated_at) VALUES(?,?,?) ON CONFLICT(workflow) DO UPDATE SET next_run_at=excluded.next_run_at,updated_at=excluded.updated_at`, workflow, next.UTC().Format(time.RFC3339Nano), now())
	return err
}
func (s *Store) ClaimScheduleRun(ctx context.Context, workflow string, scheduled time.Time) (bool, error) {
	r, err := s.db.ExecContext(ctx, `INSERT INTO schedule_runs(workflow,scheduled_at,created_at) VALUES(?,?,?) ON CONFLICT(workflow,scheduled_at) DO NOTHING`, workflow, scheduled.UTC().Format(time.RFC3339Nano), now())
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	return n == 1, err
}
func (s *Store) RunningWorkflowCount(ctx context.Context, workflow string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM executions WHERE workflow=? AND status=?`, workflow, ExecutionRunning).Scan(&count)
	return count, err
}
func (s *Store) BeginRecovery(ctx context.Context, executionID, version string, force bool) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	var n int
	if err = tx.QueryRowContext(ctx, `SELECT recovery_count FROM executions WHERE id=?`, executionID).Scan(&n); err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	n++
	if _, err = tx.ExecContext(ctx, `UPDATE executions SET status=?,error='',ended_at=NULL,recovery_count=? WHERE id=?`, ExecutionRunning, n, executionID); err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO recovery_attempts(execution_id,attempt,source_version,force_ambiguous,status,started_at) VALUES(?,?,?,?,?,?)`, executionID, n, version, boolInt(force), ExecutionRunning, now()); err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	return n, tx.Commit()
}
func (s *Store) FinishRecovery(ctx context.Context, executionID string, attempt int, status, msg string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE recovery_attempts SET status=?,error=?,ended_at=? WHERE execution_id=? AND attempt=?`, status, msg, now(), executionID, attempt)
	return err
}

func (s *Store) CreateTask(ctx context.Context, t Task) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO tasks(id,execution_id,instance_key,parent_instance_key,name,task_key,invocation_ordinal,identity_mode,status,attempt,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, t.ID, t.ExecutionID, t.InstanceKey, t.ParentInstanceKey, t.Name, t.TaskKey, t.InvocationOrdinal, t.IdentityMode, t.Status, t.Attempt, now())
	return err
}
func (s *Store) TaskByInstance(ctx context.Context, executionID, key string) (Task, error) {
	var t Task
	var result []byte
	err := s.db.QueryRowContext(ctx, `SELECT id,execution_id,instance_key,parent_instance_key,name,COALESCE(task_key,''),invocation_ordinal,identity_mode,status,attempt,result_json,COALESCE(error,'') FROM tasks WHERE execution_id=? AND instance_key=?`, executionID, key).Scan(&t.ID, &t.ExecutionID, &t.InstanceKey, &t.ParentInstanceKey, &t.Name, &t.TaskKey, &t.InvocationOrdinal, &t.IdentityMode, &t.Status, &t.Attempt, &result, &t.Error)
	t.Result = result
	return t, err
}
func (s *Store) FinishTask(ctx context.Context, id, status, msg string, result any) error {
	_, err := s.db.ExecContext(ctx, `UPDATE tasks SET status=?,error=?,result_json=?,ended_at=? WHERE id=?`, status, msg, marshal(result), now(), id)
	return err
}
func (s *Store) RestartTask(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE tasks SET status=?, attempt=attempt+1, error='', ended_at=NULL WHERE id=?`, ExecutionRunning, id)
	return err
}

func (s *Store) OperationAt(ctx context.Context, taskID string, ordinal int) (Operation, error) {
	var o Operation
	var d, r []byte
	err := s.db.QueryRowContext(ctx, `SELECT id,task_id,ordinal,fingerprint,capability,status,attempt,descriptor_json,result_json,COALESCE(idempotency_key,''),COALESCE(error,'') FROM operations WHERE task_id=? AND ordinal=?`, taskID, ordinal).Scan(&o.ID, &o.TaskID, &o.Ordinal, &o.Fingerprint, &o.Capability, &o.Status, &o.Attempt, &d, &r, &o.IdempotencyKey, &o.Error)
	o.Descriptor = d
	o.Result = r
	return o, err
}
func (s *Store) CreateOperation(ctx context.Context, o Operation) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO operations(id,task_id,ordinal,fingerprint,capability,status,attempt,descriptor_json,idempotency_key,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)`, o.ID, o.TaskID, o.Ordinal, o.Fingerprint, o.Capability, OperationPending, o.Attempt, o.Descriptor, o.IdempotencyKey, now())
	return err
}
func (s *Store) StartOperationAttempt(ctx context.Context, operationID string, attempt int, kind string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE operations SET status=?,attempt=? WHERE id=?`, OperationRunning, attempt, operationID); err == nil {
		_, err = tx.ExecContext(ctx, `INSERT INTO operation_attempts(operation_id,attempt,kind,status,started_at) VALUES(?,?,?,?,?)`, operationID, attempt, kind, OperationRunning, now())
	}
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
func (s *Store) FinishOperation(ctx context.Context, id, status string, result any, errorKind, msg string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE operations SET status=?,result_json=?,error=?,ended_at=? WHERE id=?`, status, marshal(result), msg, now(), id); err == nil {
		_, err = tx.ExecContext(ctx, `UPDATE operation_attempts SET status=?,error_kind=?,error=?,ended_at=? WHERE operation_id=? AND attempt=(SELECT attempt FROM operations WHERE id=?)`, status, errorKind, msg, now(), id, id)
	}
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *Store) Event(ctx context.Context, executionID, taskID, operationID, kind string, data any) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO events(execution_id,task_id,operation_id,type,data_json,created_at) VALUES(?,?,?,?,?,?)`, executionID, taskID, operationID, kind, marshal(data), now())
	return err
}
func (s *Store) Execution(ctx context.Context, id string) (Execution, error) {
	var e Execution
	var started string
	var ended sql.NullString
	var input, manifest, result []byte
	err := s.db.QueryRowContext(ctx, `SELECT id,workflow,version,status,input_json,artifact_manifest_json,COALESCE(result_json,X''),recovery_count,started_at,ended_at,COALESCE(error,'') FROM executions WHERE id=?`, id).Scan(&e.ID, &e.Workflow, &e.Version, &e.Status, &input, &manifest, &result, &e.RecoveryCount, &started, &ended, &e.Error)
	e.Input = input
	e.ArtifactManifest = manifest
	e.Result = result
	if err != nil {
		return e, err
	}
	e.StartedAt, _ = time.Parse(time.RFC3339Nano, started)
	if ended.Valid {
		t, _ := time.Parse(time.RFC3339Nano, ended.String)
		e.EndedAt = &t
	}
	return e, nil
}
func (s *Store) Runs(ctx context.Context, workflow string) ([]Execution, error) {
	q := `SELECT id,workflow,version,status,input_json,artifact_manifest_json,COALESCE(result_json,X''),recovery_count,started_at,ended_at,COALESCE(error,'') FROM executions`
	args := []any{}
	if workflow != "" {
		q += ` WHERE workflow=?`
		args = append(args, workflow)
	}
	q += ` ORDER BY started_at DESC LIMIT 100`
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Execution
	for rows.Next() {
		var e Execution
		var a string
		var z sql.NullString
		if err := rows.Scan(&e.ID, &e.Workflow, &e.Version, &e.Status, &e.Input, &e.ArtifactManifest, &e.Result, &e.RecoveryCount, &a, &z, &e.Error); err != nil {
			return nil, err
		}
		e.StartedAt, _ = time.Parse(time.RFC3339Nano, a)
		if z.Valid {
			t, _ := time.Parse(time.RFC3339Nano, z.String)
			e.EndedAt = &t
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
func (s *Store) Tasks(ctx context.Context, executionID string) ([]Task, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,name,status,attempt FROM tasks WHERE execution_id=? ORDER BY created_at`, executionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []Task
	for rows.Next() {
		var t Task
		if err := rows.Scan(&t.ID, &t.Name, &t.Status, &t.Attempt); err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}
func (s *Store) Operations(ctx context.Context, taskID string) ([]Operation, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,capability,status,descriptor_json FROM operations WHERE task_id=? ORDER BY ordinal`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var operations []Operation
	for rows.Next() {
		var o Operation
		if err := rows.Scan(&o.ID, &o.Capability, &o.Status, &o.Descriptor); err != nil {
			return nil, err
		}
		operations = append(operations, o)
	}
	return operations, rows.Err()
}
func (s *Store) Timeline(ctx context.Context, id string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT created_at,type,COALESCE(data_json,'{}') FROM events WHERE execution_id=? ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var t, k, d string
		if err := rows.Scan(&t, &k, &d); err != nil {
			return nil, err
		}
		out = append(out, fmt.Sprintf("%s %s %s", t, k, d))
	}
	return out, rows.Err()
}
