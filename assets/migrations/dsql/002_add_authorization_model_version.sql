-- +goose Up
-- Note: DSQL does not support ALTER COLUMN to add NOT NULL constraint after column creation
-- The NOT NULL constraint is enforced at the application layer
ALTER TABLE authorization_model ADD COLUMN schema_version TEXT DEFAULT '1.0';

-- +goose Down
ALTER TABLE authorization_model DROP COLUMN schema_version;
