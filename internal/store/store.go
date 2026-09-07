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

type Store struct{ db *sql.DB }
type Execution struct {
	ID, Workflow, Version, Status string
	StartedAt                     time.Time
	EndedAt                       *time.Time
	Error                         string
}
type Task struct {
	ID, ExecutionID, Name, TaskKey, Status string
	Attempt                                int
	Error                                  string
}
type Operation struct {
	ID, TaskID, Fingerprint, Capability, Status string
	Ordinal, Attempt                            int
	Result                                      json.RawMessage
	Error                                       string
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
		db.Close()
		return nil, err
	}
	s := &Store{db: db}
	if err := s.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}
func (s *Store) Close() error { return s.db.Close() }
func (s *Store) migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS executions (id TEXT PRIMARY KEY, workflow TEXT NOT NULL, version TEXT NOT NULL, status TEXT NOT NULL, input_json BLOB, error TEXT, started_at TEXT NOT NULL, ended_at TEXT);
CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, execution_id TEXT NOT NULL REFERENCES executions(id), name TEXT NOT NULL, task_key TEXT, status TEXT NOT NULL, attempt INTEGER NOT NULL, error TEXT, created_at TEXT NOT NULL, ended_at TEXT);
CREATE TABLE IF NOT EXISTS operations (id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(id), ordinal INTEGER NOT NULL, fingerprint TEXT NOT NULL, capability TEXT NOT NULL, status TEXT NOT NULL, attempt INTEGER NOT NULL, result_json BLOB, error TEXT, created_at TEXT NOT NULL, ended_at TEXT, UNIQUE(task_id, ordinal));
CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, execution_id TEXT NOT NULL REFERENCES executions(id), task_id TEXT, operation_id TEXT, type TEXT NOT NULL, data_json BLOB, created_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS executions_workflow_started ON executions(workflow, started_at DESC);
CREATE INDEX IF NOT EXISTS tasks_execution ON tasks(execution_id);
CREATE INDEX IF NOT EXISTS operations_task ON operations(task_id);`)
	return err
}
func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }
func (s *Store) CreateExecution(ctx context.Context, e Execution, input any) error {
	b, _ := json.Marshal(input)
	_, err := s.db.ExecContext(ctx, `INSERT INTO executions(id,workflow,version,status,input_json,started_at) VALUES(?,?,?,?,?,?)`, e.ID, e.Workflow, e.Version, e.Status, b, now())
	return err
}
func (s *Store) FinishExecution(ctx context.Context, id, status, msg string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE executions SET status=?, error=?, ended_at=? WHERE id=?`, status, msg, now(), id)
	return err
}
func (s *Store) CreateTask(ctx context.Context, t Task) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO tasks(id,execution_id,name,task_key,status,attempt,created_at) VALUES(?,?,?,?,?,?,?)`, t.ID, t.ExecutionID, t.Name, t.TaskKey, t.Status, t.Attempt, now())
	return err
}
func (s *Store) FinishTask(ctx context.Context, id, status, msg string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE tasks SET status=?, error=?, ended_at=? WHERE id=?`, status, msg, now(), id)
	return err
}
func (s *Store) BeginOperation(ctx context.Context, o Operation) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO operations(id,task_id,ordinal,fingerprint,capability,status,attempt,created_at) VALUES(?,?,?,?,?,?,?,?)`, o.ID, o.TaskID, o.Ordinal, o.Fingerprint, o.Capability, "running", o.Attempt, now())
	return err
}
func (s *Store) FinishOperation(ctx context.Context, id, status string, result any, msg string) error {
	b, _ := json.Marshal(result)
	_, err := s.db.ExecContext(ctx, `UPDATE operations SET status=?, result_json=?, error=?, ended_at=? WHERE id=?`, status, b, msg, now(), id)
	return err
}
func (s *Store) Event(ctx context.Context, executionID, taskID, operationID, kind string, data any) error {
	b, _ := json.Marshal(data)
	_, err := s.db.ExecContext(ctx, `INSERT INTO events(execution_id,task_id,operation_id,type,data_json,created_at) VALUES(?,?,?,?,?,?)`, executionID, taskID, operationID, kind, b, now())
	return err
}
func (s *Store) Execution(ctx context.Context, id string) (Execution, error) {
	var e Execution
	var started string
	var ended sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT id,workflow,version,status,started_at,ended_at,COALESCE(error,'') FROM executions WHERE id=?`, id).Scan(&e.ID, &e.Workflow, &e.Version, &e.Status, &started, &ended, &e.Error)
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
	q := `SELECT id,workflow,version,status,started_at,ended_at,COALESCE(error,'') FROM executions`
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
	out := []Execution{}
	for rows.Next() {
		var e Execution
		var a string
		var z sql.NullString
		if err := rows.Scan(&e.ID, &e.Workflow, &e.Version, &e.Status, &a, &z, &e.Error); err != nil {
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
