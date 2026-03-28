-- +goose Up

ALTER TABLE ads ADD COLUMN IF NOT EXISTS favorites INTEGER DEFAULT 0;

ALTER TABLE ad_metrics ADD COLUMN IF NOT EXISTS favorites_current INTEGER DEFAULT 0;

ALTER TABLE ad_snapshots ADD COLUMN IF NOT EXISTS favorites INTEGER DEFAULT 0;

UPDATE ads SET favorites = 0 WHERE favorites IS NULL;
UPDATE ad_metrics SET favorites_current = 0 WHERE favorites_current IS NULL;

-- +goose Down

ALTER TABLE ad_snapshots DROP COLUMN IF EXISTS favorites;
ALTER TABLE ad_metrics DROP COLUMN IF EXISTS favorites_current;
ALTER TABLE ads DROP COLUMN IF EXISTS favorites;
