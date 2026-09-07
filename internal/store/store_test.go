package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestOpenMigratesLegacyDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
CREATE TABLE executions (id TEXT PRIMARY KEY, workflow TEXT NOT NULL, version TEXT NOT NULL, status TEXT NOT NULL, input_json BLOB, error TEXT, started_at TEXT NOT NULL, ended_at TEXT);
CREATE TABLE tasks (id TEXT PRIMARY KEY, execution_id TEXT NOT NULL, name TEXT NOT NULL, task_key TEXT, status TEXT NOT NULL, attempt INTEGER NOT NULL, error TEXT, created_at TEXT NOT NULL, ended_at TEXT);
CREATE TABLE operations (id TEXT PRIMARY KEY, task_id TEXT NOT NULL, ordinal INTEGER NOT NULL, fingerprint TEXT NOT NULL, capability TEXT NOT NULL, status TEXT NOT NULL, attempt INTEGER NOT NULL, result_json BLOB, error TEXT, created_at TEXT NOT NULL, ended_at TEXT, UNIQUE(task_id, ordinal));
CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, execution_id TEXT NOT NULL, task_id TEXT, operation_id TEXT, type TEXT NOT NULL, data_json BLOB, created_at TEXT NOT NULL);`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var version int
	if err := s.db.QueryRowContext(context.Background(), `SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 3 {
		t.Fatalf("schema version = %d, want 3", version)
	}
	if _, err := s.db.Exec(`INSERT INTO executions(id,workflow,version,status,input_json,artifact_manifest_json,recovery_count,started_at) VALUES('e','w','v','failed','{}','{}',0,'2026-01-01T00:00:00Z')`); err != nil {
		t.Fatalf("new schema unavailable: %v", err)
	}
}
