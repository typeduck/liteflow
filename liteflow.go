// Package liteflow encourages keeping SQL statements in separate files. Instead
// of passing SQL and arguments, you pass a statement name and arguments. To
// generate the named statements internally, pass an io/fs.FS (e.g.: embed.FS)
// which contains the SQL files.
//
// This package can also handle database migrations for SQLite.
package liteflow

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"regexp"
	"strconv"
	"strings"
)

// DB is an enhanced SQLite *sql.DB with versioning and named statements.
type DB struct {
	*sql.DB

	// Versioning fields
	versionFS  fs.FS          // filesystem of upgrade/downgrade filenames
	upgrades   map[int]string // version => upgrade filename for version
	downgrades map[int]string // version => downgrade filename from version

	// Initializations after versioning
	initFS fs.FS

	// Prepared statement fields
	queryFS    fs.FS                // filesystem of all prepared statements
	statements map[string]*sql.Stmt // statement cache
}

// UpgradeNone indicates to skip upgrading and prepared statements.
const UpgradeNone int = -1

// UpgradeAll indicates to perform all database upgrades available.
const UpgradeAll int = 0

// Options are additional options for database upgrade.
type Options struct {
	// MaxVersion is the maximum database upgrade to run. The default zero value
	// indicates to run all available upgrades.
	MaxVersion int

	// NoPreload indicates to skip the normal preloading of all SQL queries into
	// sql.Stmt objects for later use. Setting this to true will not catch
	// errors in SQL statements until they are actually used.
	NoPreload bool

	// VersionFS is the sub-directory in the fs.FS which holds the numbered
	// database migration files.
	VersionFS fs.FS

	// InitFS is the filesystem holding initialization scripts, run in file
	// lexical sort order.
	InitFS fs.FS

	// QueryFS is the sub-directory in the fs.FS which holds all prepared
	// statements.
	QueryFS fs.FS
}

// defaultOptions are the default options used when no options are provided.
var defaultOptions = Options{}

// New creates DB, which is an enhanced *sql.DB with version control and named
// prepared statements.
//
// See Options documentation for the available configurations.
//
// If the returned database is non-nil, it may still be usable even if there
// were errors.
func New(db *sql.DB, opts *Options) (*DB, error) {
	if db == nil {
		return nil, fmt.Errorf("non-nil *sql.DB reference required")
	}

	var errs []error // collect errors and join in the last step.
	if opts == nil {
		opts = &defaultOptions
	}

	d := &DB{
		DB:         db,
		versionFS:  opts.VersionFS,
		upgrades:   make(map[int]string),
		downgrades: make(map[int]string),
		initFS:     opts.InitFS,
		queryFS:    opts.QueryFS,
		statements: make(map[string]*sql.Stmt),
	}

	// Load the Versioning map to prepare for upgrade.
	if d.versionFS != nil {
		errs = append(errs, d.loadVersions()...)
	}

	// Upgrade database unless inhibited.
	if d.versionFS != nil && opts.MaxVersion != UpgradeNone {
		_, err := d.Upgrade(opts.MaxVersion)
		errs = append(errs, err)
	}

	// Run any initialization scripts.
	if d.initFS != nil && !opts.NoPreload {
		errs = append(errs, d.runInitScripts()...)
	}

	// Preload Statements unless inhibited.
	if d.queryFS != nil && !opts.NoPreload {
		loaderrs := d.loadAllStatements()
		errs = append(errs, loaderrs...)
	}

	return d, errors.Join(errs...)
}

// loadVersions initializes the map of the version numbers to the appropriate
// upgrade & downgrade filenames.
func (db *DB) loadVersions() []error {
	entries, err := fs.ReadDir(db.versionFS, ".")
	if err != nil {
		return []error{fmt.Errorf("could not read versionFS: %w", err)}
	}

	rxVersion := regexp.MustCompile(`\d+`)
	rxUpgrade := regexp.MustCompile(`\.up\.`)
	rxDowngrade := regexp.MustCompile(`\.down\.`)

	var errs []error
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		vnum, _ := strconv.Atoi(rxVersion.FindString(entry.Name()))
		if vnum > 0 {
			if rxUpgrade.MatchString(entry.Name()) {
				db.upgrades[vnum] = entry.Name()
			} else if rxDowngrade.MatchString(entry.Name()) {
				db.downgrades[vnum] = entry.Name()
			}
		}
	}
	return errs
}

// runInitScripts runs any SQL-based start initializations prior to loading
// statements.
func (db *DB) runInitScripts() []error {
	entries, err := fs.ReadDir(db.initFS, ".")
	if err != nil {
		return []error{fmt.Errorf("could not read InitFS: %w", err)}
	}
	var errs []error
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		if err := db.runOneScript(entry.Name()); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

// runOneScript runs a single initialization script
func (db *DB) runOneScript(filename string) error {
	f, err := db.initFS.Open(filename)
	if err != nil {
		return fmt.Errorf("could not open SQL init file '%s': %w", filename, err)
	}
	content, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("could not read SQL init file '%s': %w", filename, err)
	}
	tx, err := db.DB.Begin()
	if err != nil {
		return fmt.Errorf("could not start tx for init file '%s': %w", filename, err)
	}
	if _, err := tx.Exec(string(content)); err != nil {
		tx.Rollback()
		return fmt.Errorf("could not run SQL init file '%s' (rolled back): %w", filename, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit for init file '%s': %w", filename, err)
	}
	return nil
}

// loadAllStatements loads all the embedded prepared statements
func (db *DB) loadAllStatements() []error {
	entries, err := fs.ReadDir(db.queryFS, ".")
	if err != nil {
		return []error{fmt.Errorf("could not read QueryFS: %w", err)}
	}
	var errs []error
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), ".sql")
		if err := db.loadStatements(name); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

var rxNameline = regexp.MustCompile(`(?i)^--\s*name:\s*(\S+)`)
var rxComment = regexp.MustCompile(`^--`)

// loadStatements loads a single SQL file's named statement into the cache.
func (db *DB) loadStatements(name string) error {
	if db.queryFS == nil {
		return fmt.Errorf("no QueryFS provided, cannot load statement '%s'", name)
	}
	// TODO: fix lazy-loading when statement is named in comment.
	fn := name + ".sql"
	f, err := db.queryFS.Open(fn)
	if err != nil {
		return fmt.Errorf("could not open file '%s': %w", fn, err)
	}
	s := bufio.NewScanner(f)
	subname := ""
	lines := make([]string, 0, 20)
	lno := 0
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if len(line) == 0 {
			continue
		}
		lno++
		if rxNameline.MatchString(line) { // found named query
			// prepare any buffered lines into current statement.
			if len(lines) > 0 {
				joined := strings.Join(lines, "\n")
				queryName := name
				if subname != "" {
					queryName += "." + subname
				}
				s, err := db.DB.Prepare(joined)
				if err != nil {
					return fmt.Errorf("[%s:%d] could not prepare '%s': %w", fn, lno, queryName, err)
				}
				db.statements[queryName] = s
			}
			// extract the new subname for the following lines / reset buffer
			parts := rxNameline.FindStringSubmatch(line)
			subname = parts[1]
			lines = lines[:0]
		} else if !rxComment.MatchString(line) {
			lines = append(lines, line)
		}
	}
	if s.Err() != nil {
		return fmt.Errorf("could not scan %s (reached line %d): %w", fn, lno, s.Err())
	}
	// If lines left over (e.g. no named queries), then prepare query now.
	if len(lines) > 0 {
		joined := strings.Join(lines, "\n")
		queryName := name
		if subname != "" {
			queryName += "." + subname
		}
		s, err := db.DB.Prepare(joined)
		if err != nil {
			return fmt.Errorf("[%s:%d] could not prepare '%s': %w", fn, lno, queryName, err)
		}
		db.statements[queryName] = s

	}
	return nil
}

// Version returns the current database version.
func (db *DB) Version() (int, error) {
	var vCurr int
	row := db.DB.QueryRow("PRAGMA user_version")
	if err := row.Scan(&vCurr); err != nil {
		return vCurr, fmt.Errorf("could not query user_version: %w", err)
	}
	return vCurr, nil
}

// Upgrade increments the database to at _most_ the given version. The actual
// version and any error are returned. Passing a version of zero will upgrade as
// far as possible.
func (db *DB) Upgrade(version int) (int, error) {
	var vCurr int
	for {
		var err error
		if vCurr, err = db.Version(); err != nil {
			return vCurr, err
		}
		if version > 0 && vCurr >= version {
			break
		}
		vNext := vCurr + 1
		nextFileName, ok := db.upgrades[vNext]
		if !ok {
			break
		}
		if err := db.runFileAndSetVersion(nextFileName, vNext); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return vCurr, nil
			}
			return vCurr, err
		}
	}
	return vCurr, nil
}

// Downgrade decrements the database to the given version. The actual version
// and any error are returned.
func (db *DB) Downgrade(version int) (int, error) {
	var vCurr int
	for {
		var err error
		if vCurr, err = db.Version(); err != nil {
			return vCurr, err
		}
		if vCurr <= version {
			break
		}
		nextFileName, ok := db.downgrades[vCurr]
		if !ok {
			break
		}
		if err := db.runFileAndSetVersion(nextFileName, vCurr-1); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return vCurr, nil
			}
			return vCurr, err
		}
	}
	return vCurr, nil
}

// runFileAndSetVersion runs the contents of an SQL file and sets the
// user_version of the database in a single transaction. An error is returned if
// the operation did not succeed.
func (db *DB) runFileAndSetVersion(filename string, version int) error {
	f, err := db.versionFS.Open(filename)
	if err != nil {
		return fmt.Errorf("could not open SQL file '%s': %w", filename, err)
	}

	content, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("could not read SQL file '%s': %w", filename, err)
	}
	tx, err := db.DB.Begin()
	if err != nil {
		return fmt.Errorf("could not start tx for file '%s': %w", filename, err)
	}
	if _, err := tx.Exec(string(content)); err != nil {
		tx.Rollback()
		return fmt.Errorf("could not run SQL file '%s: %w", filename, err)
	}
	vUpdateSql := fmt.Sprintf("PRAGMA user_version = %d", version)
	if _, err := tx.Exec(vUpdateSql); err != nil {
		tx.Rollback()
		return fmt.Errorf("could not update user_version to %d: %w", version, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit for file %s, version %d: %w", filename, version, err)
	}
	return nil
}

// named returns the named statement.
func (db *DB) named(name string) (*sql.Stmt, error) {
	s, ok := db.statements[name]
	if !ok {
		var err error
		err = db.loadStatements(name)
		if err != nil {
			return nil, fmt.Errorf("could not load statement '%s': %w", name, err)
		}
		s = db.statements[name]
	}
	return s, nil
}

// Exec is sql.DB.Exec but with a query name.
func (db *DB) Exec(name string, args ...any) (sql.Result, error) {
	s, err := db.named(name)
	if err != nil {
		return nil, err
	}
	return s.Exec(args...)
}

// ExecContext is sql.DB.ExecContext but with a query name.
func (db *DB) ExecContext(ctx context.Context, name string, args ...any) (sql.Result, error) {
	s, err := db.named(name)
	if err != nil {
		return nil, err
	}
	return s.ExecContext(ctx, args...)
}

// Query is sql.DB.Query but with a query name.
func (db *DB) Query(name string, args ...any) (*sql.Rows, error) {
	s, err := db.named(name)
	if err != nil {
		return nil, err
	}
	return s.Query(args...)
}

// QueryContext is sql.DB.QueryContext but with a query name.
func (db *DB) QueryContext(ctx context.Context, name string, args ...any) (*sql.Rows, error) {
	s, err := db.named(name)
	if err != nil {
		return nil, err
	}
	return s.QueryContext(ctx, args...)
}

// QueryRow is sql.DB.QueryRow but with a query name.
func (db *DB) QueryRow(name string, args ...any) (*sql.Row, error) {
	s, err := db.named(name)
	if err != nil {
		return nil, err
	}
	return s.QueryRow(args...), nil
}

// QueryRowContext is sql.DB.QueryRowContext but with a query name.
func (db *DB) QueryRowContext(ctx context.Context, name string, args ...any) (*sql.Row, error) {
	s, err := db.named(name)
	if err != nil {
		return nil, err
	}
	return s.QueryRowContext(ctx, args...), nil
}

// Begin is like sql.DB.Begin, but returns a *liteflow.Tx for named queries.
func (db *DB) Begin() (*Tx, error) {
	return db.BeginTx(context.Background(), nil)
}

// BeginTx is like sql.DB.BeginTx, but returns a *liteflow.Tx for named queries.
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := db.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("could not begin transaction: %w", err)
	}
	return &Tx{
		Tx:         tx,
		DB:         db,
		statements: map[string]*sql.Stmt{},
	}, nil
}

// Tx is like sql.Tx but uses named parameters.
type Tx struct {
	*sql.Tx
	DB         *DB
	statements map[string]*sql.Stmt
}

// named creates a prepared statement for use within the transaction.
//
// The name must correspond to statements file.
func (tx *Tx) named(name string) (*sql.Stmt, error) {
	s, ok := tx.statements[name]
	if !ok {
		var err error
		if s, err = tx.DB.named(name); err != nil {
			return nil, err
		}
		s = tx.Stmt(s)
		tx.statements[name] = s
	}
	return s, nil
}

// Exec is like sql.Tx.Exec but with a query name.
func (tx *Tx) Exec(name string, args ...any) (sql.Result, error) {
	s, err := tx.named(name)
	if err != nil {
		return nil, err
	}
	return s.Exec(args...)
}

// ExecContext is like sql.Tx.ExecContext but with a query name.
func (tx *Tx) ExecContext(ctx context.Context, name string, args ...any) (sql.Result, error) {
	s, err := tx.named(name)
	if err != nil {
		return nil, err
	}
	return s.ExecContext(ctx, args...)
}

// Query is like sql.Tx.Query but with a query name.
func (tx *Tx) Query(name string, args ...any) (*sql.Rows, error) {
	s, err := tx.named(name)
	if err != nil {
		return nil, err
	}
	return s.Query(args...)
}

// QueryContext is like sql.Tx.QueryContext but with a query name.
func (tx *Tx) QueryContext(ctx context.Context, name string, args ...any) (*sql.Rows, error) {
	s, err := tx.named(name)
	if err != nil {
		return nil, err
	}
	return s.QueryContext(ctx, args...)
}

// QueryRow is like sql.Tx.QueryRow but with a query name.
func (tx *Tx) QueryRow(name string, args ...any) (*sql.Row, error) {
	s, err := tx.named(name)
	if err != nil {
		return nil, err
	}
	return s.QueryRow(args...), nil
}

// QueryRowContext is like sql.Tx.QueryRowContext but with a query name.
func (tx *Tx) QueryRowContext(ctx context.Context, name string, args ...any) (*sql.Row, error) {
	s, err := tx.named(name)
	if err != nil {
		return nil, err
	}
	return s.QueryRowContext(ctx, args...), nil
}
