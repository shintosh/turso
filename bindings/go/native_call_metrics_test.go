package turso

import (
	"database/sql"
	"testing"
)

func TestNativeCallMetricsDescribeCompletedDatabaseWork(t *testing.T) {
	before := ReadNativeCallMetrics()
	db, err := sql.Open("turso", ":memory:")
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(t.Context(), "CREATE TABLE metrics_probe(id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create metrics probe: %v", err)
	}
	if _, err := db.ExecContext(t.Context(), "INSERT INTO metrics_probe(id) VALUES (1)"); err != nil {
		t.Fatalf("insert metrics probe: %v", err)
	}
	var count int
	if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM metrics_probe").Scan(&count); err != nil {
		t.Fatalf("read metrics probe: %v", err)
	}
	if count != 1 {
		t.Fatalf("probe count = %d, want 1", count)
	}
	after := ReadNativeCallMetrics()
	if after.Calls <= before.Calls {
		t.Fatalf("native calls did not advance: before=%d after=%d", before.Calls, after.Calls)
	}
	if after.ExecutionTime <= before.ExecutionTime {
		t.Fatalf("native execution time did not advance: before=%s after=%s", before.ExecutionTime, after.ExecutionTime)
	}
	if after.ObjectLocks <= before.ObjectLocks {
		t.Fatalf("native object locks did not advance: before=%d after=%d", before.ObjectLocks, after.ObjectLocks)
	}
	if after.ObjectWaitTime <= before.ObjectWaitTime {
		t.Fatalf("native object wait did not advance: before=%s after=%s", before.ObjectWaitTime, after.ObjectWaitTime)
	}
}
