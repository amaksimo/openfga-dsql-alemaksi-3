-- +goose Up
CREATE INDEX ASYNC idx_reverse_lookup_user ON tuple (store, object_type, relation, _user);

-- +goose Down
DROP INDEX IF EXISTS idx_reverse_lookup_user;
