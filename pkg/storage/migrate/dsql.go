package migrate

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws-samples/aurora-dsql-samples/go/dsql-pgx-connector/dsql"
	"github.com/pressly/goose/v3"

	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/pkg/logger"
)

// dsqlMigrationConfig holds DSQL-specific migration configuration.
type dsqlMigrationConfig struct {
	driver         string
	migrationsPath string
	uri            string
}

// prepareDSQLMigration prepares the migration configuration for DSQL.
// It generates an IAM auth token and sets up the goose version table.
func prepareDSQLMigration(uri string, log logger.Logger) (*dsqlMigrationConfig, error) {
	ctx := context.Background()

	// Generate IAM auth token
	token, err := dsql.GenerateTokenConnString(ctx, uri)
	if err != nil {
		return nil, fmt.Errorf("generate DSQL auth token: %w", err)
	}

	// Convert dsql:// to postgres:// with token as password
	pgURI := "postgres" + strings.TrimPrefix(uri, "dsql")
	dbURI, err := url.Parse(pgURI)
	if err != nil {
		return nil, fmt.Errorf("parse database URI: %w", err)
	}

	username := "admin"
	if dbURI.User != nil {
		username = dbURI.User.Username()
	}
	dbURI.User = url.UserPassword(username, token)

	// DSQL requires SSL; remove region param which PostgreSQL doesn't understand
	q := dbURI.Query()
	q.Set("sslmode", "require")
	q.Del("region")
	dbURI.RawQuery = q.Encode()

	finalURI := dbURI.String()

	// Pre-create goose table (DSQL doesn't support SERIAL/IDENTITY)
	if err := ensureGooseTableForDSQL(finalURI, log); err != nil {
		return nil, fmt.Errorf("create goose version table: %w", err)
	}

	log.Info("using DSQL datastore with IAM authentication")

	return &dsqlMigrationConfig{
		driver:         "pgx",
		migrationsPath: assets.DSQLMigrationDir,
		uri:            finalURI,
	}, nil
}

// ensureGooseTableForDSQL creates the goose_db_version table if it doesn't exist.
// DSQL doesn't support SERIAL/IDENTITY, so we use BIGINT with epoch microseconds.
func ensureGooseTableForDSQL(uri string, log logger.Logger) error {
	db, err := goose.OpenDBWithDriver("pgx", uri)
	if err != nil {
		return fmt.Errorf("open connection: %w", err)
	}
	defer db.Close()

	// Create table if not exists - uses BIGINT id with epoch microseconds for ordering
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS goose_db_version (
			id BIGINT PRIMARY KEY DEFAULT (EXTRACT(EPOCH FROM now()) * 1000000)::BIGINT,
			version_id BIGINT NOT NULL,
			is_applied BOOLEAN NOT NULL,
			tstamp TIMESTAMP DEFAULT now()
		)
	`)
	if err != nil {
		return fmt.Errorf("create goose table: %w", err)
	}

	// Goose expects an initial row with version 0 to exist
	var hasRows bool
	if err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM goose_db_version)`).Scan(&hasRows); err != nil {
		return fmt.Errorf("check goose table: %w", err)
	}
	if !hasRows {
		if _, err := db.Exec(`INSERT INTO goose_db_version (version_id, is_applied) VALUES (0, TRUE)`); err != nil {
			return fmt.Errorf("insert initial goose row: %w", err)
		}
	}

	log.Info("ensured goose_db_version table exists for DSQL")
	return nil
}
