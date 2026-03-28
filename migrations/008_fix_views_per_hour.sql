-- +goose Up

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
        CASE
            WHEN s.snap_count < 2 THEN 0
            WHEN s.hours_tracked >= 24 THEN
                (s.views_now - COALESCE(s.views_24h, s.views_first))::double precision / GREATEST(s.hours_tracked, 1)
            ELSE
                (s.views_now - s.views_first)::double precision / GREATEST(s.hours_tracked, 1)
        END,
        COALESCE(s.price_prev, s.price_now),
        s.price_min,
        s.price_max,
        (s.price_now < COALESCE(s.price_prev, s.price_now) AND s.price_now > 0 AND COALESCE(s.price_prev, 0) > 0),
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
            agg.views_first,
            agg.price_min,
            agg.price_max,
            agg.snap_count,
            agg.first_ts,
            agg.last_ts,
            GREATEST(EXTRACT(EPOCH FROM (agg.last_ts - agg.first_ts)) / 3600.0, 1) AS hours_tracked
        FROM (
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
                MIN(views) AS views_first,
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

-- Revert to original formula (total views / hours)
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
        s.ad_id, s.views_now, s.price_now,
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
        s.price_min, s.price_max,
        (s.price_now < COALESCE(s.price_prev, s.price_now) AND s.price_now > 0),
        CASE WHEN COALESCE(s.price_prev, 0) > 0
            THEN ((s.price_now - s.price_prev) / s.price_prev * 100)
            ELSE 0 END,
        s.snap_count, s.first_ts, s.last_ts, NOW()
    FROM (
        SELECT
            latest.ad_id,
            latest.views AS views_now, latest.price_eur AS price_now,
            h1.views AS views_1h, h24.views AS views_24h, d7.views AS views_7d,
            prev.price_eur AS price_prev,
            agg.price_min, agg.price_max, agg.snap_count,
            agg.first_ts, agg.last_ts,
            GREATEST(EXTRACT(EPOCH FROM (agg.last_ts - agg.first_ts)) / 3600.0, 1) AS hours_tracked
        FROM (
            SELECT DISTINCT ON (ad_id) ad_id, views, price_eur, ts
            FROM ad_snapshots WHERE ts >= NOW() - INTERVAL '2 hours'
            ORDER BY ad_id, ts DESC
        ) latest
        LEFT JOIN LATERAL (SELECT views FROM ad_snapshots WHERE ad_id = latest.ad_id AND ts <= NOW() - INTERVAL '1 hour' ORDER BY ts DESC LIMIT 1) h1 ON true
        LEFT JOIN LATERAL (SELECT views FROM ad_snapshots WHERE ad_id = latest.ad_id AND ts <= NOW() - INTERVAL '24 hours' ORDER BY ts DESC LIMIT 1) h24 ON true
        LEFT JOIN LATERAL (SELECT views FROM ad_snapshots WHERE ad_id = latest.ad_id AND ts <= NOW() - INTERVAL '7 days' ORDER BY ts DESC LIMIT 1) d7 ON true
        LEFT JOIN LATERAL (SELECT price_eur FROM ad_snapshots WHERE ad_id = latest.ad_id AND ts < latest.ts AND price_eur != latest.price_eur ORDER BY ts DESC LIMIT 1) prev ON true
        LEFT JOIN LATERAL (
            SELECT MIN(price_eur) FILTER (WHERE price_eur > 0) AS price_min, MAX(price_eur) AS price_max,
                COUNT(*) AS snap_count, MIN(ts) AS first_ts, MAX(ts) AS last_ts
            FROM ad_snapshots WHERE ad_id = latest.ad_id
        ) agg ON true
    ) s
    WHERE EXISTS (SELECT 1 FROM ads WHERE id = s.ad_id)
    ON CONFLICT (ad_id) DO UPDATE SET
        views_current = EXCLUDED.views_current, price_current = EXCLUDED.price_current,
        views_1h_ago = EXCLUDED.views_1h_ago, views_24h_ago = EXCLUDED.views_24h_ago, views_7d_ago = EXCLUDED.views_7d_ago,
        views_delta_1h = EXCLUDED.views_delta_1h, views_delta_24h = EXCLUDED.views_delta_24h, views_delta_7d = EXCLUDED.views_delta_7d,
        views_per_hour = EXCLUDED.views_per_hour, price_previous = EXCLUDED.price_previous,
        price_min_seen = LEAST(ad_metrics.price_min_seen, EXCLUDED.price_min_seen),
        price_max_seen = GREATEST(ad_metrics.price_max_seen, EXCLUDED.price_max_seen),
        price_dropped = EXCLUDED.price_dropped, price_change_pct = EXCLUDED.price_change_pct,
        snapshot_count = EXCLUDED.snapshot_count, last_snapshot_at = EXCLUDED.last_snapshot_at, updated_at = NOW();
END;
$$ LANGUAGE plpgsql;
