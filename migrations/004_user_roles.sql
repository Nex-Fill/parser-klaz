-- +goose Up

ALTER TABLE users ADD COLUMN IF NOT EXISTS role TEXT DEFAULT 'free';
UPDATE users SET role = 'admin' WHERE is_admin = true;

-- +goose Down
ALTER TABLE users DROP COLUMN IF EXISTS role;
