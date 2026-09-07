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
	ID, Workflow, Version, Status string
	Input, ArtifactManifest       json.RawMessage
	RecoveryCount                 int
	StartedAt                     time.Time
	EndedAt                       *time.Time
	Error                         string
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
func (s *Store) FinishExecution(ctx context.Context, id, status, msg string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE executions SET status=?, error=?, ended_at=? WHERE id=?`, status, msg, now(), id)
	return err
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
	var input, manifest []byte
	err := s.db.QueryRowContext(ctx, `SELECT id,workflow,version,status,input_json,artifact_manifest_json,recovery_count,started_at,ended_at,COALESCE(error,'') FROM executions WHERE id=?`, id).Scan(&e.ID, &e.Workflow, &e.Version, &e.Status, &input, &manifest, &e.RecoveryCount, &started, &ended, &e.Error)
	e.Input = input
	e.ArtifactManifest = manifest
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
	q := `SELECT id,workflow,version,status,input_json,artifact_manifest_json,recovery_count,started_at,ended_at,COALESCE(error,'') FROM executions`
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
		if err := rows.Scan(&e.ID, &e.Workflow, &e.Version, &e.Status, &e.Input, &e.ArtifactManifest, &e.RecoveryCount, &a, &z, &e.Error); err != nil {
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
