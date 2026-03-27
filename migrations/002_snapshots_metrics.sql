-- +goose Up

-- ==================== TimescaleDB ====================
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ==================== SNAPSHOTS (hypertable) ====================
-- Append-only time-series. NO upsert, NO conflicts. Just INSERT.
-- 50M inserts/hour = ~14K/sec — TimescaleDB handles this easily.

CREATE TABLE ad_snapshots (
    ad_id TEXT NOT NULL,
    views INTEGER NOT NULL DEFAULT 0,
    price_eur DOUBLE PRECISION NOT NULL DEFAULT 0,
    ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Convert to hypertable: auto-partitioned by time, 1-day chunks
SELECT create_hypertable('ad_snapshots', by_range('ts', INTERVAL '1 day'));

-- Index for per-ad queries (charts)
CREATE INDEX idx_snap_ad_ts ON ad_snapshots (ad_id, ts DESC);

-- Compression: chunks older than 7 days get compressed ~10-20x
ALTER TABLE ad_snapshots SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'ad_id',
    timescaledb.compress_orderby = 'ts DESC'
);
SELECT add_compression_policy('ad_snapshots', INTERVAL '7 days');

-- Retention: drop data older than 90 days (hourly aggregates cover the rest)
SELECT add_retention_policy('ad_snapshots', INTERVAL '90 days');

-- ==================== CONTINUOUS AGGREGATES ====================
-- TimescaleDB auto-maintains these. Zero manual cron jobs.

-- Hourly stats (auto-refreshed)
CREATE MATERIALIZED VIEW ad_snapshots_hourly
WITH (timescaledb.continuous) AS
SELECT
    ad_id,
    time_bucket('1 hour', ts) AS hour,
    MIN(views) AS views_min,
    MAX(views) AS views_max,
    AVG(views)::INTEGER AS views_avg,
    MAX(views) - MIN(views) AS views_delta,
    last(price_eur, ts) AS price_last,
    first(price_eur, ts) AS price_first,
    COUNT(*) AS snap_count
FROM ad_snapshots
GROUP BY ad_id, time_bucket('1 hour', ts);

SELECT add_continuous_aggregate_policy('ad_snapshots_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour'
);

-- Daily stats (auto-refreshed, built on top of hourly)
CREATE MATERIALIZED VIEW ad_snapshots_daily
WITH (timescaledb.continuous) AS
SELECT
    ad_id,
    time_bucket('1 day', ts) AS day,
    MIN(views) AS views_min,
    MAX(views) AS views_max,
    AVG(views)::INTEGER AS views_avg,
    MAX(views) - MIN(views) AS views_delta,
    last(price_eur, ts) AS price_last,
    first(price_eur, ts) AS price_first,
    COUNT(*) AS snap_count
FROM ad_snapshots
GROUP BY ad_id, time_bucket('1 day', ts);

SELECT add_continuous_aggregate_policy('ad_snapshots_daily',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day'
);

-- ==================== AD METRICS (batch-refreshed) ====================
-- Updated every 5 minutes by a background Go job.
-- NOT updated per-insert. That's the key difference.
-- This is the table that makes search-by-dynamics instant.

CREATE TABLE ad_metrics (
    ad_id TEXT PRIMARY KEY REFERENCES ads(id) ON DELETE CASCADE,

    views_current INTEGER DEFAULT 0,
    price_current DOUBLE PRECISION DEFAULT 0,

    views_1h_ago INTEGER DEFAULT 0,
    views_24h_ago INTEGER DEFAULT 0,
    views_7d_ago INTEGER DEFAULT 0,

    views_delta_1h INTEGER DEFAULT 0,
    views_delta_24h INTEGER DEFAULT 0,
    views_delta_7d INTEGER DEFAULT 0,
    views_per_hour DOUBLE PRECISION DEFAULT 0,

    price_previous DOUBLE PRECISION DEFAULT 0,
    price_min_seen DOUBLE PRECISION DEFAULT 0,
    price_max_seen DOUBLE PRECISION DEFAULT 0,
    price_dropped BOOLEAN DEFAULT false,
    price_change_pct DOUBLE PRECISION DEFAULT 0,

    snapshot_count INTEGER DEFAULT 0,
    first_seen_at TIMESTAMPTZ DEFAULT NOW(),
    last_snapshot_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_metrics_d1h ON ad_metrics (views_delta_1h DESC) WHERE views_delta_1h > 0;
CREATE INDEX idx_metrics_d24h ON ad_metrics (views_delta_24h DESC) WHERE views_delta_24h > 0;
CREATE INDEX idx_metrics_vph ON ad_metrics (views_per_hour DESC) WHERE views_per_hour > 0;
CREATE INDEX idx_metrics_price_drop ON ad_metrics (price_dropped) WHERE price_dropped = true;
CREATE INDEX idx_metrics_last ON ad_metrics (last_snapshot_at DESC);

-- ==================== BATCH REFRESH FUNCTION ====================
-- Called from Go every 5 minutes. Processes ALL ads at once via SQL.
-- One query refreshes millions of rows — no per-row overhead.

CREATE OR REPLACE FUNCTION refresh_ad_metrics() RETURNS VOID AS $$
BEGIN
    INSERT INTO ad_metrics (
        ad_id, views_current, price_current,
        views_1h_ago, views_24h_ago, views_7d_ago,
        views_delta_1h, views_delta_24h, views_delta_7d,
        views_per_hour,
        price_previous, price_min_seen, price_max_seen,
        price_dropped, price_change_pct,
        snapshot_count, first_seen_at, last_snapshot_at, updated_at
    )
    SELECT
        s.ad_id,
        s.views_now,
        s.price_now,
        COALESCE(s.views_1h, s.views_now),
        COALESCE(s.views_24h, s.views_now),
        COALESCE(s.views_7d, s.views_now),
        s.views_now - COALESCE(s.views_1h, s.views_now),
        s.views_now - COALESCE(s.views_24h, s.views_now),
        s.views_now - COALESCE(s.views_7d, s.views_now),
        CASE WHEN s.hours_tracked > 0
            THEN s.views_now::double precision / s.hours_tracked
            ELSE 0 END,
        COALESCE(s.price_prev, s.price_now),
        s.price_min,
        s.price_max,
        (s.price_now < COALESCE(s.price_prev, s.price_now) AND s.price_now > 0),
        CASE WHEN COALESCE(s.price_prev, 0) > 0
            THEN ((s.price_now - s.price_prev) / s.price_prev * 100)
            ELSE 0 END,
        s.snap_count,
        s.first_ts,
        s.last_ts,
        NOW()
    FROM (
        SELECT
            latest.ad_id,
            latest.views AS views_now,
            latest.price_eur AS price_now,
            h1.views AS views_1h,
            h24.views AS views_24h,
            d7.views AS views_7d,
            prev.price_eur AS price_prev,
            agg.price_min,
            agg.price_max,
            agg.snap_count,
            agg.first_ts,
            agg.last_ts,
            GREATEST(EXTRACT(EPOCH FROM (agg.last_ts - agg.first_ts)) / 3600.0, 1) AS hours_tracked
        FROM (
            -- Latest snapshot per ad
            SELECT DISTINCT ON (ad_id) ad_id, views, price_eur, ts
            FROM ad_snapshots
            WHERE ts >= NOW() - INTERVAL '2 hours'
            ORDER BY ad_id, ts DESC
        ) latest
        LEFT JOIN LATERAL (
            SELECT views FROM ad_snapshots
            WHERE ad_id = latest.ad_id AND ts <= NOW() - INTERVAL '1 hour'
            ORDER BY ts DESC LIMIT 1
        ) h1 ON true
        LEFT JOIN LATERAL (
            SELECT views FROM ad_snapshots
            WHERE ad_id = latest.ad_id AND ts <= NOW() - INTERVAL '24 hours'
            ORDER BY ts DESC LIMIT 1
        ) h24 ON true
        LEFT JOIN LATERAL (
            SELECT views FROM ad_snapshots
            WHERE ad_id = latest.ad_id AND ts <= NOW() - INTERVAL '7 days'
            ORDER BY ts DESC LIMIT 1
        ) d7 ON true
        LEFT JOIN LATERAL (
            SELECT price_eur FROM ad_snapshots
            WHERE ad_id = latest.ad_id AND ts < latest.ts AND price_eur != latest.price_eur
            ORDER BY ts DESC LIMIT 1
        ) prev ON true
        LEFT JOIN LATERAL (
            SELECT
                MIN(price_eur) FILTER (WHERE price_eur > 0) AS price_min,
                MAX(price_eur) AS price_max,
                COUNT(*) AS snap_count,
                MIN(ts) AS first_ts,
                MAX(ts) AS last_ts
            FROM ad_snapshots WHERE ad_id = latest.ad_id
        ) agg ON true
    ) s
    WHERE EXISTS (SELECT 1 FROM ads WHERE id = s.ad_id)
    ON CONFLICT (ad_id) DO UPDATE SET
        views_current = EXCLUDED.views_current,
        price_current = EXCLUDED.price_current,
        views_1h_ago = EXCLUDED.views_1h_ago,
        views_24h_ago = EXCLUDED.views_24h_ago,
        views_7d_ago = EXCLUDED.views_7d_ago,
        views_delta_1h = EXCLUDED.views_delta_1h,
        views_delta_24h = EXCLUDED.views_delta_24h,
        views_delta_7d = EXCLUDED.views_delta_7d,
        views_per_hour = EXCLUDED.views_per_hour,
        price_previous = EXCLUDED.price_previous,
        price_min_seen = LEAST(ad_metrics.price_min_seen, EXCLUDED.price_min_seen),
        price_max_seen = GREATEST(ad_metrics.price_max_seen, EXCLUDED.price_max_seen),
        price_dropped = EXCLUDED.price_dropped,
        price_change_pct = EXCLUDED.price_change_pct,
        snapshot_count = EXCLUDED.snapshot_count,
        last_snapshot_at = EXCLUDED.last_snapshot_at,
        updated_at = NOW();
END;
$$ LANGUAGE plpgsql;

-- +goose Down
DROP FUNCTION IF EXISTS refresh_ad_metrics();
DROP TABLE IF EXISTS ad_metrics;
DROP MATERIALIZED VIEW IF EXISTS ad_snapshots_daily;
DROP MATERIALIZED VIEW IF EXISTS ad_snapshots_hourly;
DROP TABLE IF EXISTS ad_snapshots;
