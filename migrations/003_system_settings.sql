-- +goose Up

CREATE TABLE system_settings (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL DEFAULT '""',
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    updated_by TEXT DEFAULT ''
);

INSERT INTO system_settings (key, value) VALUES
    ('recheck_fresh_interval', '"30m"'),
    ('recheck_popular_interval', '"1h"'),
    ('recheck_normal_interval', '"4h"'),
    ('recheck_archive_interval', '"24h"'),
    ('parse_workers', '50'),
    ('image_workers', '20'),
    ('proxy_source_url', '"https://sh.govno.de/proxies/text?country=Germany&maxTimeout=2000"'),
    ('proxy_update_interval', '"5m"'),
    ('auto_recheck_enabled', 'true'),
    ('image_upload_enabled', 'true'),
    ('category_blacklist', '[]'),
    ('free_tier_task_limit', '3'),
    ('premium_tier_task_limit', '20')
ON CONFLICT DO NOTHING;

-- +goose Down
DROP TABLE IF EXISTS system_settings;
