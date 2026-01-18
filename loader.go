package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/uptrace/bun/dialect/mysqldialect"
	libsql "github.com/webcore-go/lib-sql"
	"github.com/webcore-go/webcore/app/config"
	"github.com/webcore-go/webcore/app/loader"

	_ "github.com/go-sql-driver/mysql"
)

type MysqlLoader struct {
	name string
}

func (a *MysqlLoader) SetName(name string) {
	a.name = name
}

func (a *MysqlLoader) Name() string {
	return a.name
}

func (l *MysqlLoader) Init(args ...any) (loader.Library, error) {
	config := args[1].(config.DatabaseConfig)
	dsn := libsql.BuildDSN(config)

	db := &libsql.SQLDatabase{}

	driver := libsql.NewConnector("mysql", &Connector{dsn: dsn})
	dialect := mysqldialect.New()

	// Set up Bun SQL database wrapper
	db.SetBunDB(driver, dialect)

	err := db.Install(args...)
	if err != nil {
		return nil, err
	}

	db.Connect()

	// l.DB = db
	return db, nil
}

// ----------------------- Connector -------------------

// Connector wraps the MySQL standard driver
type Connector struct {
	dsn string
}

var _ driver.Connector = (*Connector)(nil)

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	db, err := sql.Open("mysql", c.dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open mysql: %w", err)
	}

	// Verify connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping mysql: %w", err)
	}

	return &mysqlConn{db: db}, nil
}

func (c *Connector) Driver() driver.Driver {
	return libsql.NewDriver()
}

// mysqlConn wraps the MySQL database connection
type mysqlConn struct {
	db *sql.DB
}

func (c *mysqlConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.db.PrepareContext(context.Background(), query)
	if err != nil {
		return nil, err
	}
	return &mysqlStmt{stmt: stmt}, nil
}

func (c *mysqlConn) Close() error {
	return c.db.Close()
}

func (c *mysqlConn) Begin() (driver.Tx, error) {
	tx, err := c.db.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, err
	}
	return &mysqlTx{tx: tx}, nil
}

// mysqlStmt wraps the MySQL statement
type mysqlStmt struct {
	stmt *sql.Stmt
}

func (s *mysqlStmt) Close() error {
	return s.stmt.Close()
}

func (s *mysqlStmt) NumInput() int {
	return -1
}

func (s *mysqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	result, err := s.stmt.ExecContext(context.Background(), libsql.ToNamedValues(args)...)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *mysqlStmt) Query(args []driver.Value) (driver.Rows, error) {
	rows, err := s.stmt.QueryContext(context.Background(), libsql.ToNamedValues(args)...)
	if err != nil {
		return nil, err
	}
	return &mysqlRows{rows: rows}, nil
}

// mysqlTx wraps the MySQL transaction
type mysqlTx struct {
	tx *sql.Tx
}

func (t *mysqlTx) Commit() error {
	return t.tx.Commit()
}

func (t *mysqlTx) Rollback() error {
	return t.tx.Rollback()
}

// mysqlRows wraps the MySQL rows
type mysqlRows struct {
	rows *sql.Rows
}

func (r *mysqlRows) Columns() []string {
	cols, _ := r.rows.Columns()
	return cols
}

func (r *mysqlRows) Close() error {
	return r.rows.Close()
}

func (r *mysqlRows) Next(dest []driver.Value) error {
	// Convert []driver.Value to []any
	args := make([]any, len(dest))
	for i := range dest {
		args[i] = &dest[i]
	}
	return r.rows.Scan(args...)
}
