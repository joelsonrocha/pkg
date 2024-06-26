package pkg

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func MigrateDatabase() error {
	postgresUrl := os.Getenv("POSTGRES_URL")
	migrationPath := os.Getenv("MIGRATION_PATH")

	fmt.Println("variáveis", postgresUrl, migrationPath)

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
	fmt.Println("Abrindo conexão com banco:")
	conn, err := sql.Open("postgres", os.Getenv("POSTGRES_URL"))
	if err != nil {
		fmt.Println("failed to open database connection:", err)
		return nil, fmt.Errorf("failed to open database connection")
		//panic(err)
	}

	return conn, err
}

func CloseConnection(conn *sql.DB) error {
	err := conn.Close()
	if err != nil {
		return fmt.Errorf("failed to close database connection: %w", err)
	}
	return nil
}
