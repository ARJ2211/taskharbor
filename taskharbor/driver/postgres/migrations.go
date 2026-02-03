package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

/*
We will use go:embed to embed the
migration files during compile time
so it can be accessible @ run time.
*/
var migrationFS embed.FS

/*
This will be stored in th_schema_migrations
and version can be the filename (001, 002, etc)
*/
type migration struct {
	version  string
	filename string
	sql      string
}

/*
This function will apply all embedded SQL
migrations in the version order.
  - reads applied version from th_schema_migrations
  - executes each migration inside a transaction
  - records the migration version on success
*/
func ApplyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if pool == nil {
		return ErrNilPool
	}

	migs, err := loadMigrations()
	if err != nil {
		return err
	}

	applied, err := loadAppliedMigrations(ctx, pool)
	if err != nil {
		return nil
	}

	for _, m := range migs {
		if applied[m.version] == true {
			continue
		}

		if err := applyOne(ctx, pool, m); err != nil {
			return err
		}
	}

	return nil
}

/*
This function will load the migrations
as a FS and sort it and return a list
of applicable migrations
*/
func loadMigrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return nil, err
	}

	var migs []migration
	for _, e := range entries {
		if e.IsDir() {
			continue
		}

		name := e.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}

		b, err := migrationFS.ReadFile(
			path.Join("migrations", name),
		)
		if err != nil {
			return nil, err
		}

		migs = append(migs, migration{
			version:  name,
			filename: name,
			sql:      string(b),
		})
	}

	// Sort in lexicographical order
	sort.Slice(migs, func(i, j int) bool {
		return migs[i].filename < migs[j].filename
	})

	return migs, nil
}

/*
This function will return a set of
migration versions that have already
been applied by querying th_schema_migrations

(If table does not exist, run the migrations)
*/
func loadAppliedMigrations(
	ctx context.Context, pool *pgxpool.Pool,
) (map[string]bool, error) {
	rows, err := pool.Query(ctx, "SELECT version FROM th_schema_migrations")
	if err != nil {
		// Fresh DB; table does not exist.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			return map[string]bool{}, nil
		}
		return nil, err
	}
	defer rows.Close()

	applied := map[string]bool{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf(
				"scan th_schema_migrations row: %w", err)
		}
		applied[v] = true
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate th_schema_migrations: %w", err)
	}

	return applied, nil
}

/*
This function will apply one migrations
file in a TRANSACTION.

STEPS:
------

	1 - Begin a transaction
	2 - Split SQL into individual statements
	3 - Execute each statement in an orfedr
	4 - Record the migration version in th_schema_migrations
	5 - Commit
*/
func applyOne(
	ctx context.Context,
	pool *pgxpool.Pool,
	m migration,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	stmts := splitSQLStatements(m.sql)
	for _, stmt := range stmts {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("migration %s exec failed: %w", m.filename, err)
		}
	}

	if _, err := tx.Exec(ctx, `INSERT INTO th_schema_migrations(version) VALUES ($1)`, m.version); err != nil {
		return fmt.Errorf("migration %s record version failed: %w", m.filename, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration %s: %w", m.filename, err)
	}

	return nil
}

/*
This function will split a migration file into
executable SQL statements.
*/
func splitSQLStatements(s string) []string {
	var out []string
	var b strings.Builder

	inSingle := false
	inDouble := false
	inLineComment := false
	inBlockComment := false
	dollarTag := ""

	for i := 0; i < len(s); i++ {
		ch := s[i]
		var next byte
		if i+1 < len(s) {
			next = s[i+1]
		}

		if inLineComment {
			if ch == '\n' {
				inLineComment = false
			}
			continue
		}
		if inBlockComment {
			if ch == '*' && next == '/' {
				inBlockComment = false
				i++
			}
			continue
		}

		if dollarTag != "" {
			if ch == '$' && strings.HasPrefix(s[i:], dollarTag) {
				b.WriteString(dollarTag)
				i += len(dollarTag) - 1
				dollarTag = ""
				continue
			}
			b.WriteByte(ch)
			continue
		}

		if !inSingle && !inDouble {
			if ch == '-' && next == '-' {
				inLineComment = true
				i++
				continue
			}
			if ch == '/' && next == '*' {
				inBlockComment = true
				i++
				continue
			}
		}

		if !inSingle && ch == '$' {
			j := i + 1
			for j < len(s) && isDollarTagChar(s[j]) {
				j++
			}
			if j < len(s) && s[j] == '$' {
				tag := s[i : j+1] // includes both $ ... $
				dollarTag = tag
				b.WriteString(tag)
				i = j
				continue
			}
		}

		if ch == '\'' && !inDouble {
			if inSingle && next == '\'' {
				b.WriteByte(ch)
				b.WriteByte(next)
				i++
				continue
			}
			inSingle = !inSingle
			b.WriteByte(ch)
			continue
		}

		if ch == '"' && !inSingle {
			inDouble = !inDouble
			b.WriteByte(ch)
			continue
		}

		if ch == ';' && !inSingle && !inDouble && dollarTag == "" {
			stmt := strings.TrimSpace(b.String())
			if stmt != "" {
				out = append(out, stmt)
			}
			b.Reset()
			continue
		}

		b.WriteByte(ch)
	}

	last := strings.TrimSpace(b.String())
	if last != "" {
		out = append(out, last)
	}

	return out
}

func isDollarTagChar(b byte) bool {
	return (b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9') ||
		b == '_'
}
