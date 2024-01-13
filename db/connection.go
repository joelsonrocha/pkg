package db

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/golang-migrate/migrate"
	"github.com/golang-migrate/migrate/database/postgres"
)

func MigrateDatabase() error {
	postgresUrl := os.Getenv("POSTGRES_URL")
	migrationPath := os.Getenv("MIGRATION_PATH")

	if postgresUrl == "" {
		return fmt.Errorf("POSTGRES_URL variable is not available")
	}

	if migrationPath == "" {
		return fmt.Errorf("MIGRATION_PATH variable is not available")
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
		"file://"+migrationPath,
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
