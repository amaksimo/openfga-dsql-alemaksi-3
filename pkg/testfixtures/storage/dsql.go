package storage

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver.
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage/migrate"
	"github.com/openfga/openfga/pkg/storage/postgres"
)

// dsqlTestContainer implements DatastoreTestContainer for Aurora DSQL.
// Unlike other test containers, this connects to a real DSQL cluster
// specified via environment variables.
type dsqlTestContainer struct {
	clusterEndpoint string
	region          string
	version         int64
}

// NewDSQLTestContainer returns an implementation of the DatastoreTestContainer interface
// for Aurora DSQL. Requires OPENFGA_DSQL_CLUSTER_ENDPOINT and AWS_REGION environment variables.
func NewDSQLTestContainer() *dsqlTestContainer {
	return &dsqlTestContainer{}
}

func (d *dsqlTestContainer) GetDatabaseSchemaVersion() int64 {
	return d.version
}

// RunDSQLTestContainer connects to a DSQL cluster and runs migrations.
// Environment variables required:
//   - OPENFGA_DSQL_CLUSTER_ENDPOINT: The DSQL cluster endpoint (e.g., "abc123.dsql.us-east-1.on.aws")
//   - AWS_REGION: The AWS region (e.g., "us-east-1")
func (d *dsqlTestContainer) RunDSQLTestContainer(t testing.TB) DatastoreTestContainer {
	clusterEndpoint := os.Getenv("OPENFGA_DSQL_CLUSTER_ENDPOINT")
	if clusterEndpoint == "" {
		t.Skip("OPENFGA_DSQL_CLUSTER_ENDPOINT not set, skipping DSQL tests")
	}

	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = os.Getenv("AWS_DEFAULT_REGION")
	}
	if region == "" {
		t.Skip("AWS_REGION not set, skipping DSQL tests")
	}

	d.clusterEndpoint = clusterEndpoint
	d.region = region

	// Run migrations using the migrate package
	uri := d.GetConnectionURI(false)
	err := migrate.RunMigrations(migrate.MigrationConfig{
		Engine:  "dsql",
		URI:     uri,
		Timeout: 2 * time.Minute,
	})
	require.NoError(t, err, "failed to run DSQL migrations")

	// Get the schema version
	d.version = d.getSchemaVersion(t)

	t.Cleanup(func() {
		pgURI, err := postgres.PrepareDSQLURI(d.GetConnectionURI(false))
		if err != nil {
			t.Logf("failed to prepare DSQL URI for cleanup: %v", err)
			return
		}

		db, err := goose.OpenDBWithDriver("pgx", pgURI)
		if err != nil {
			t.Logf("failed to connect for cleanup: %v", err)
			return
		}
		defer db.Close()

		tables := []string{"changelog", "tuple", "assertion", "authorization_model", "store"}
		for _, table := range tables {
			if _, err := db.Exec("DELETE FROM " + table); err != nil {
				t.Logf("failed to clean up table %s: %v", table, err)
			}
		}

		t.Log("DSQL test cleanup complete")
	})

	return d
}

func (d *dsqlTestContainer) getSchemaVersion(t testing.TB) int64 {
	pgURI, err := postgres.PrepareDSQLURI(d.GetConnectionURI(false))
	require.NoError(t, err, "failed to prepare DSQL URI")

	goose.SetLogger(goose.NopLogger())

	db, err := goose.OpenDBWithDriver("pgx", pgURI)
	require.NoError(t, err)
	defer db.Close()

	backoffPolicy := backoff.NewExponentialBackOff()
	backoffPolicy.MaxElapsedTime = 30 * time.Second

	var version int64
	err = backoff.Retry(func() error {
		var dbErr error
		version, dbErr = goose.GetDBVersion(db)
		return dbErr
	}, backoffPolicy)
	require.NoError(t, err, "failed to get schema version")

	return version
}

// GetConnectionURI returns the DSQL connection URI.
func (d *dsqlTestContainer) GetConnectionURI(includeCredentials bool) string {
	// DSQL uses IAM authentication, so credentials are not included in the URI
	// The dsql:// scheme triggers IAM token generation in the connector
	return fmt.Sprintf("dsql://admin@%s/postgres?region=%s", d.clusterEndpoint, d.region)
}

func (d *dsqlTestContainer) GetUsername() string {
	return "admin"
}

func (d *dsqlTestContainer) GetPassword() string {
	// DSQL uses IAM authentication, no static password
	return ""
}

// CreateSecondary is not supported for DSQL (it's a distributed database).
func (d *dsqlTestContainer) CreateSecondary(t testing.TB) error {
	return fmt.Errorf("secondary datastores not supported for DSQL")
}

// GetSecondaryConnectionURI returns empty string as DSQL doesn't support secondary connections.
func (d *dsqlTestContainer) GetSecondaryConnectionURI(includeCredentials bool) string {
	return ""
}
