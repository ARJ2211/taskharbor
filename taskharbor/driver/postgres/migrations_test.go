package postgres

import "testing"

func TestSplitSQLStatements(t *testing.T) {
	sql := `
-- comment
CREATE TABLE a (id TEXT);
CREATE TABLE b (id TEXT, v TEXT DEFAULT 'x;y');
/* block
comment */
CREATE TABLE c (id TEXT);
`
	stmts := splitSQLStatements(sql)
	if len(stmts) != 3 {
		t.Fatalf("expected 3 statements, got %d: %#v", len(stmts), stmts)
	}
}
