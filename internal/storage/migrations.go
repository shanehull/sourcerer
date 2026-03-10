package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
)

type Migration struct {
	Name string
	Up   func(ctx context.Context, db *sql.DB) error
}

var migrations = []Migration{
	{
		Name: "001_create_leads_table",
		Up:   migration001CreateLeadsTable,
	},
	{
		Name: "002_add_google_places_columns",
		Up:   migration002AddGooglePlacesColumns,
	},
}

func migration001CreateLeadsTable(ctx context.Context, db *sql.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS leads (
		abn TEXT PRIMARY KEY,
		name TEXT,
		category TEXT,
		sources TEXT,
		entity_type TEXT,
		entity_status TEXT,
		state TEXT,
		postcode TEXT,
		registration_date TIMESTAMP,
		age_years INTEGER,
		gst_registered BOOLEAN,
		gst_effective_from TIMESTAMP,
		is_current_entity BOOLEAN,
		acn TEXT,
		main_trading_name TEXT,
		phone TEXT,
		email TEXT,
		business_url TEXT,
		found_at_url TEXT,
		updated_at TIMESTAMP
	);`
	_, err := db.ExecContext(ctx, query)
	return err
}

func migration002AddGooglePlacesColumns(ctx context.Context, db *sql.DB) error {
	columns := []struct {
		name string
		typ  string
	}{
		{"google_places_id", "TEXT"},
		{"google_formatted_name", "TEXT"},
		{"google_phone", "TEXT"},
		{"google_website", "TEXT"},
		{"google_formatted_addr", "TEXT"},
		{"google_primary_type", "TEXT"},
		{"google_types", "TEXT"},
		{"google_rating", "DOUBLE"},
		{"google_rating_count", "INTEGER"},
	}

	for _, col := range columns {
		checkQuery := fmt.Sprintf(`
			SELECT COUNT(*) FROM information_schema.columns 
			WHERE table_name = 'leads' AND column_name = '%s'
		`, col.name)
		var count int
		if err := db.QueryRowContext(ctx, checkQuery).Scan(&count); err != nil {
			return err
		}

		if count == 0 {
			alterQuery := fmt.Sprintf(`ALTER TABLE leads ADD COLUMN %s %s`, col.name, col.typ)
			if _, err := db.ExecContext(ctx, alterQuery); err != nil {
				return err
			}
		}
	}
	return nil
}

func RunMigrations(ctx context.Context, db *sql.DB, logger *slog.Logger) error {
	// Create migrations table if it doesn't exist
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			name TEXT PRIMARY KEY,
			applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`); err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	// Run each migration that hasn't been applied yet
	for _, migration := range migrations {
		var applied bool
		err := db.QueryRowContext(ctx, `
			SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE name = ?)
		`, migration.Name).Scan(&applied)

		if err != nil {
			return fmt.Errorf("failed to check migration status: %w", err)
		}

		if applied {
			logger.Debug("Migration already applied", "name", migration.Name)
			continue
		}

		logger.Info("Running migration", "name", migration.Name)
		if err := migration.Up(ctx, db); err != nil {
			return fmt.Errorf("migration %s failed: %w", migration.Name, err)
		}

		// Record migration as applied
		if _, err := db.ExecContext(ctx, `
			INSERT INTO schema_migrations (name) VALUES (?)
		`, migration.Name); err != nil {
			return fmt.Errorf("failed to record migration %s: %w", migration.Name, err)
		}
	}

	return nil
}
