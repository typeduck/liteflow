// Package liteflow encourages keeping SQL statements in separate files. It is
// used with an fs.FS (recommended: embed.FS) which keeps database migrations
// and prepared statements in sub-folders.
package liteflow

import (
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

	// Preload Statements unless inhibited.
	if d.queryFS != nil && !opts.NoPreload {
		loaderrs := d.loadStatements()
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

// loadStatements loads all the embedded prepared statements
func (db *DB) loadStatements() []error {
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
		if err := db.loadStatement(name); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

// loadStatement loads a single named statement into the cache.
func (db *DB) loadStatement(name string) error {
	if db.queryFS == nil {
		return fmt.Errorf("no QueryFS provided, cannot load statement '%s'", name)
	}
	f := name + ".sql"
	b, err := fs.ReadFile(db.queryFS, f)
	if err != nil {
		return fmt.Errorf("could not read file '%s': %w", f, err)
	}
	s, err := db.DB.Prepare(string(b))
	if err != nil {
		return fmt.Errorf("could not prepare '%s': %w", name, err)
	}
	db.statements[name] = s
	return nil
}

// Version returns the current database version according to the ./versions.
func (db *DB) Version() (int, error) {
	var vCurr int
	row := db.QueryRow("PRAGMA user_version")
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
func (db *DB) Named(name string) (*sql.Stmt, error) {
	s, ok := db.statements[name]
	if !ok {
		var err error
		err = db.loadStatement(name)
		if err != nil {
			return nil, fmt.Errorf("could not load statement '%s': %w", name, err)
		}
		s = db.statements[name]
	}
	return s, nil
}

func (db *DB) NamedExec(name string, args ...any) (sql.Result, error) {
	s, err := db.Named(name)
	if err != nil {
		return nil, err
	}
	return s.Exec(args...)
}

func (db *DB) NamedExecContext(ctx context.Context, name string, args ...any) (sql.Result, error) {
	s, err := db.Named(name)
	if err != nil {
		return nil, err
	}
	return s.ExecContext(ctx, args...)
}

// NamedQuery executes a query that returns rows.
func (db *DB) NamedQuery(name string, args ...any) (*sql.Rows, error) {
	s, err := db.Named(name)
	if err != nil {
		return nil, err
	}
	return s.Query(args...)
}

func (db *DB) NamedQueryContext(ctx context.Context, name string, args ...any) (*sql.Rows, error) {
	s, err := db.Named(name)
	if err != nil {
		return nil, err
	}
	return s.QueryContext(ctx, args...)
}

func (db *DB) NamedQueryRow(name string, args ...any) (*sql.Row, error) {
	s, err := db.Named(name)
	if err != nil {
		return nil, err
	}
	return s.QueryRow(args...), nil
}

func (db *DB) NamedQueryRowContext(ctx context.Context, name string, args ...any) (*sql.Row, error) {
	s, err := db.Named(name)
	if err != nil {
		return nil, err
	}
	return s.QueryRowContext(ctx, args...), nil
}

// Tx starts a new transaction for data modifications.
func (db *DB) Begin() (*Tx, error) {
	return db.BeginTx(context.Background(), nil)
}

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

// Tx is required for all data access and modification.
type Tx struct {
	*sql.Tx
	DB         *DB
	statements map[string]*sql.Stmt
}

// Named creates a prepared statement for use within the transaction.
//
// The name must correspond to statements file.
func (tx *Tx) Named(name string) (*sql.Stmt, error) {
	s, ok := tx.statements[name]
	if !ok {
		var err error
		if s, err = tx.DB.Named(name); err != nil {
			return nil, err
		}
		s = tx.Stmt(s)
		tx.statements[name] = s
	}
	return s, nil
}

func (tx *Tx) NamedExec(name string, args ...any) (sql.Result, error) {
	s, err := tx.Named(name)
	if err != nil {
		return nil, err
	}
	return s.Exec(args...)
}

func (tx *Tx) NamedExecContext(ctx context.Context, name string, args ...any) (sql.Result, error) {
	s, err := tx.Named(name)
	if err != nil {
		return nil, err
	}
	return s.ExecContext(ctx, args...)
}

func (tx *Tx) NamedQuery(name string, args ...any) (*sql.Rows, error) {
	s, err := tx.Named(name)
	if err != nil {
		return nil, err
	}
	return s.Query(args...)
}

func (tx *Tx) NamedQueryContext(ctx context.Context, name string, args ...any) (*sql.Rows, error) {
	s, err := tx.Named(name)
	if err != nil {
		return nil, err
	}
	return s.QueryContext(ctx, args...)
}

func (tx *Tx) NamedQueryRow(name string, args ...any) (*sql.Row, error) {
	s, err := tx.Named(name)
	if err != nil {
		return nil, err
	}
	return s.QueryRow(args...), nil
}

func (tx *Tx) NamedQueryRowContext(ctx context.Context, name string, args ...any) (*sql.Row, error) {
	s, err := tx.Named(name)
	if err != nil {
		return nil, err
	}
	return s.QueryRowContext(ctx, args...), nil
}
