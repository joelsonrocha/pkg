package pkg

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

type ConnectOptions struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxIdle     time.Duration
	ConnMaxLifetime time.Duration
	ForceUTC        bool
	QueryLogger     QueryLogger
}

type QueryLogger func(ctx context.Context, query string, args ...any)

type SqlcFactory[T any] func(db *sql.DB) T

var logger QueryLogger = DefaultLogger

func MigrateDatabase() error {
	postgresUrl := os.Getenv("POSTGRES_URL")
	migrationPath := os.Getenv("MIGRATION_PATH")

	if postgresUrl == "" {
		return fmt.Errorf("POSTGRES_URL variable is not available")
	}

	if migrationPath == "" {
		return fmt.Errorf("MIGRATION_PATH variable is not available")
	}

	absoluteMigrationPath, err := filepath.Abs(migrationPath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path for migration: %w", err)
	}

	conn, err := sql.Open("postgres", os.Getenv("POSTGRES_URL"))
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}
	defer conn.Close()

	driver, err := postgres.WithInstance(conn, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("failed to create database driver: %w", err)
	}

	m, err := migrate.NewWithDatabaseInstance(
		"file://"+absoluteMigrationPath,
		"postgres", driver)
	if err != nil {
		return fmt.Errorf("failed to create migration instance: %w", err)
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("failed to apply migrations: %w", err)
	}

	return nil
}

func OpenConnection() (*sql.DB, error) {
	postgresUrl := os.Getenv("POSTGRES_URL")

	if postgresUrl == "" {
		return nil, fmt.Errorf("POSTGRES_URL variable is not available")
	}
	conn, err := sql.Open("postgres", os.Getenv("POSTGRES_URL"))
	if err != nil {
		fmt.Println("failed to open database connection:", err)
		return nil, fmt.Errorf("failed to open database connection")
		//panic(err)
	}

	return conn, err
}

func CloseConnection(conn *sql.DB) error {
	return conn.Close()
}

func OpenConnectionV2(ctx context.Context, opts ConnectOptions) (*sql.DB, error) {
	if opts.DSN == "" {
		opts.DSN = os.Getenv("POSTGRES_URL")
	}
	if opts.DSN == "" {
		return nil, fmt.Errorf("DSN not provided and POSTGRES_URL not set")
	}

	db, err := sql.Open("postgres", opts.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	db.SetMaxOpenConns(opts.MaxOpenConns)
	db.SetMaxIdleConns(opts.MaxIdleConns)
	db.SetConnMaxIdleTime(opts.ConnMaxIdle)
	db.SetConnMaxLifetime(opts.ConnMaxLifetime)

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	if opts.ForceUTC {
		if _, err := db.ExecContext(ctx, "SET TIMEZONE='UTC'"); err != nil {
			return nil, fmt.Errorf("failed to set timezone: %w", err)
		}
	}

	return db, nil
}

func NewSqlcWithConnection[T any](ctx context.Context, factory SqlcFactory[T], opts ConnectOptions) (T, *sql.DB, error) {
	db, err := OpenConnectionV2(ctx, opts)
	if err != nil {
		var zero T
		return zero, nil, err
	}
	return factory(db), db, nil
}

func DefaultLogger(ctx context.Context, query string, args ...any) {
	fmt.Printf("[Query] %s | args: %v\n", query, args)
}

func ExecWithLog(ctx context.Context, db *sql.DB, query string, args ...any) (sql.Result, error) {
	start := time.Now()
	if logger != nil {
		logger(ctx, query, args...)
	}
	res, err := db.ExecContext(ctx, query, args...)
	fmt.Printf("[Exec] Took: %s | Error: %v\n", time.Since(start), err)
	return res, err
}

func QueryWithLog(ctx context.Context, db *sql.DB, query string, args ...any) (*sql.Rows, error) {
	start := time.Now()
	if logger != nil {
		logger(ctx, query, args...)
	}
	rows, err := db.QueryContext(ctx, query, args...)
	fmt.Printf("[Query] Took: %s | Error: %v\n", time.Since(start), err)
	return rows, err
}

func QueryRowWithLog(ctx context.Context, db *sql.DB, query string, args ...any) *sql.Row {
	if logger != nil {
		logger(ctx, query, args...)
	}
	return db.QueryRowContext(ctx, query, args...)
}
