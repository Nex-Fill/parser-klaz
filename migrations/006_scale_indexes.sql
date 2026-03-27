-- +goose Up

DROP INDEX IF EXISTS idx_ads_title_trgm;
DROP INDEX IF EXISTS idx_ads_description_trgm;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ads_title_trgm_active
    ON ads USING gin (title gin_trgm_ops)
    WHERE is_active = true AND is_deleted = false;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ads_desc_trgm_active
    ON ads USING gin (description gin_trgm_ops)
    WHERE is_active = true AND is_deleted = false;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tasks_user_status
    ON parse_tasks(user_id, status, created_at DESC);

-- +goose Down
DROP INDEX IF EXISTS idx_ads_title_trgm_active;
DROP INDEX IF EXISTS idx_ads_desc_trgm_active;
DROP INDEX IF EXISTS idx_tasks_user_status;
