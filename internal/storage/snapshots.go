package storage

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	kl "github.com/danamakarenko/klaz-parser/pkg/kleinanzeigen"
)

// SnapshotBuffer collects snapshots and flushes them in bulk via COPY protocol.
// This is the key to handling 14K+ inserts/sec for 50M ads.
type SnapshotBuffer struct {
	db        *Postgres
	mu        sync.Mutex
	buf       []snapshot
	flushSize int
	flushTick *time.Ticker
	done      chan struct{}
}

type snapshot struct {
	AdID      string
	Views     int
	Favorites int
	PriceEUR  float64
	TS        time.Time
}

func NewSnapshotBuffer(db *Postgres, flushSize int, flushInterval time.Duration) *SnapshotBuffer {
	sb := &SnapshotBuffer{
		db:        db,
		buf:       make([]snapshot, 0, flushSize*2),
		flushSize: flushSize,
		flushTick: time.NewTicker(flushInterval),
		done:      make(chan struct{}),
	}
	go sb.flushLoop()
	return sb
}

func (sb *SnapshotBuffer) RecordFull(adID string, views, favorites int, price float64) {
	sb.mu.Lock()
	sb.buf = append(sb.buf, snapshot{adID, views, favorites, price, time.Now()})
	needFlush := len(sb.buf) >= sb.flushSize
	sb.mu.Unlock()

	if needFlush {
		go sb.flush()
	}
}

func (sb *SnapshotBuffer) flushLoop() {
	for {
		select {
		case <-sb.flushTick.C:
			sb.flush()
		case <-sb.done:
			sb.flush()
			return
		}
	}
}

func (sb *SnapshotBuffer) flush() {
	sb.mu.Lock()
	if len(sb.buf) == 0 {
		sb.mu.Unlock()
		return
	}
	batch := sb.buf
	sb.buf = make([]snapshot, 0, sb.flushSize*2)
	sb.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := sb.db.BulkInsertSnapshots(ctx, batch); err != nil {
		log.Error().Err(err).Int("count", len(batch)).Msg("snapshot flush failed")
		return
	}
	log.Debug().Int("count", len(batch)).Msg("snapshots flushed")
}

func (sb *SnapshotBuffer) Stop() {
	sb.flushTick.Stop()
	close(sb.done)
}

// BulkInsertSnapshots uses PostgreSQL COPY protocol — handles 100K+ rows/sec.
// Zero conflicts: snapshots are append-only, no UPSERT.
func (p *Postgres) BulkInsertSnapshots(ctx context.Context, snaps []snapshot) error {
	rows := make([][]interface{}, len(snaps))
	for i, s := range snaps {
		rows[i] = []interface{}{s.AdID, s.Views, s.Favorites, s.PriceEUR, s.TS}
	}

	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	_, err = conn.Conn().CopyFrom(
		ctx,
		pgx.Identifier{"ad_snapshots"},
		[]string{"ad_id", "views", "favorites", "price_eur", "ts"},
		pgx.CopyFromRows(rows),
	)
	return err
}

// RefreshMetrics calls the SQL function that batch-recomputes ALL metrics at once.
// Called every 5 minutes from a background job. One SQL query → millions of rows.
func (p *Postgres) RefreshMetrics(ctx context.Context) error {
	_, err := p.pool.Exec(ctx, "SELECT refresh_ad_metrics()")
	return err
}

// ==================== QUERIES FOR API ====================

func (p *Postgres) GetAdMetrics(ctx context.Context, adID string) (*kl.AdMetrics, error) {
	var m kl.AdMetrics
	err := p.pool.QueryRow(ctx, `
		SELECT ad_id, views_current, COALESCE(favorites_current, 0), price_current,
			views_1h_ago, views_24h_ago, views_7d_ago,
			views_delta_1h, views_delta_24h, views_delta_7d, views_per_hour,
			COALESCE(favorites_1h_ago, 0), COALESCE(favorites_24h_ago, 0), COALESCE(favorites_7d_ago, 0),
			COALESCE(favorites_delta_1h, 0), COALESCE(favorites_delta_24h, 0), COALESCE(favorites_delta_7d, 0),
			COALESCE(favorites_per_hour, 0),
			price_previous, price_min_seen, price_max_seen,
			price_dropped, price_change_pct,
			snapshot_count, first_seen_at, last_snapshot_at
		FROM ad_metrics WHERE ad_id = $1`, adID,
	).Scan(
		&m.AdID, &m.ViewsCurrent, &m.FavoritesCurrent, &m.PriceCurrent,
		&m.Views1hAgo, &m.Views24hAgo, &m.Views7dAgo,
		&m.ViewsDelta1h, &m.ViewsDelta24h, &m.ViewsDelta7d, &m.ViewsPerHour,
		&m.Favorites1hAgo, &m.Favorites24hAgo, &m.Favorites7dAgo,
		&m.FavoritesDelta1h, &m.FavoritesDelta24h, &m.FavoritesDelta7d,
		&m.FavoritesPerHour,
		&m.PricePrevious, &m.PriceMinSeen, &m.PriceMaxSeen,
		&m.PriceDropped, &m.PriceChangePct,
		&m.SnapshotCount, &m.FirstSeenAt, &m.LastSnapshotAt,
	)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// GetAdChartData returns chart data for a single ad.
// Resolution auto-selects: raw snapshots for ≤7d, hourly for ≤90d, daily for >90d.
func (p *Postgres) GetAdChartData(ctx context.Context, adID string) (*kl.AdChartData, error) {
	chart := &kl.AdChartData{AdID: adID}

	// Raw snapshots (last 7 days) — for zoom-in detail
	rows, err := p.pool.Query(ctx, `
		SELECT views, COALESCE(favorites, 0), price_eur, ts
		FROM ad_snapshots
		WHERE ad_id = $1 AND ts >= NOW() - INTERVAL '7 days'
		ORDER BY ts ASC`, adID)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var views, favs int
			var price float64
			var ts time.Time
			if rows.Scan(&views, &favs, &price, &ts) == nil {
				chart.ViewsChart = append(chart.ViewsChart, kl.ChartPoint{Timestamp: ts, Value: float64(views)})
				chart.FavoritesChart = append(chart.FavoritesChart, kl.ChartPoint{Timestamp: ts, Value: float64(favs)})
				chart.PriceChart = append(chart.PriceChart, kl.ChartPoint{Timestamp: ts, Value: price})
			}
		}
	}

	// Hourly aggregates (all time) — for long-term graph
	hRows, err := p.pool.Query(ctx, `
		SELECT hour, views_avg, views_delta, price_last
		FROM ad_snapshots_hourly
		WHERE ad_id = $1
		ORDER BY hour ASC`, adID)
	if err == nil {
		defer hRows.Close()
		for hRows.Next() {
			var hour time.Time
			var avg, delta int
			var price float64
			if hRows.Scan(&hour, &avg, &delta, &price) == nil {
				chart.HourlyViews = append(chart.HourlyViews, kl.ChartPoint{Timestamp: hour, Value: float64(avg)})
				chart.HourlyDelta = append(chart.HourlyDelta, kl.ChartPoint{Timestamp: hour, Value: float64(delta)})
				chart.HourlyPrice = append(chart.HourlyPrice, kl.ChartPoint{Timestamp: hour, Value: price})
			}
		}
	}

	// Daily aggregates (all time) — for overview
	dRows, err := p.pool.Query(ctx, `
		SELECT day, views_avg, views_delta, price_last
		FROM ad_snapshots_daily
		WHERE ad_id = $1
		ORDER BY day ASC`, adID)
	if err == nil {
		defer dRows.Close()
		for dRows.Next() {
			var day time.Time
			var avg, delta int
			var price float64
			if dRows.Scan(&day, &avg, &delta, &price) == nil {
				chart.DailyViews = append(chart.DailyViews, kl.ChartPoint{Timestamp: day, Value: float64(avg)})
				chart.DailyDelta = append(chart.DailyDelta, kl.ChartPoint{Timestamp: day, Value: float64(delta)})
			}
		}
	}

	return chart, nil
}

// SearchAdsWithMetrics — the main search API.
// JOIN ads + ad_metrics for instant filtering by dynamics.
func (p *Postgres) SearchAdsWithMetrics(ctx context.Context, req kl.AdSearchRequest) ([]kl.AdWithMetrics, int, error) {
	where := []string{"1=1"}
	args := []interface{}{}
	n := 1

	add := func(clause string, val interface{}) {
		where = append(where, fmt.Sprintf(clause, n))
		args = append(args, val)
		n++
	}

	hasTextQuery := req.Query != ""
	if hasTextQuery {
		words := splitSearchWords(req.Query)
		if len(words) == 1 {
			where = append(where, fmt.Sprintf("(a.title ILIKE $%d OR a.description ILIKE $%d)", n, n))
			args = append(args, "%"+words[0]+"%")
			n++
		} else {
			var wordClauses []string
			for _, w := range words {
				wordClauses = append(wordClauses, fmt.Sprintf("(a.title ILIKE $%d OR a.description ILIKE $%d)", n, n))
				args = append(args, "%"+w+"%")
				n++
			}
			where = append(where, "("+joinWhere(wordClauses)+")")
		}
	}
	if len(req.Exclude) > 0 {
		var exWords []string
		for _, ex := range req.Exclude {
			ex = strings.TrimSpace(ex)
			if ex == "" {
				continue
			}
			safe := strings.ReplaceAll(ex, "\\", "\\\\")
			safe = strings.ReplaceAll(safe, ".", "\\.")
			safe = strings.ReplaceAll(safe, "*", "\\*")
			safe = strings.ReplaceAll(safe, "+", "\\+")
			safe = strings.ReplaceAll(safe, "?", "\\?")
			safe = strings.ReplaceAll(safe, "(", "\\(")
			safe = strings.ReplaceAll(safe, ")", "\\)")
			safe = strings.ReplaceAll(safe, "[", "\\[")
			safe = strings.ReplaceAll(safe, "|", "\\|")
			exWords = append(exWords, safe)
		}
		if len(exWords) > 0 {
			pattern := strings.Join(exWords, "|")
			where = append(where, fmt.Sprintf("a.title !~* $%d", n))
			args = append(args, pattern)
			n++
		}
	}
	if len(req.CategoryIDs) > 0 {
		expanded := p.expandCategoryIDs(ctx, req.CategoryIDs)
		quoted := make([]string, len(expanded))
		for i, id := range expanded {
			safe := strings.Map(func(r rune) rune {
				if r >= '0' && r <= '9' {
					return r
				}
				return -1
			}, id)
			quoted[i] = "'" + safe + "'"
		}
		where = append(where, "a.category_id IN ("+strings.Join(quoted, ",")+")")
	}
	if len(req.LocationIDs) > 0 {
		add("a.location_id = ANY($%d)", req.LocationIDs)
	}
	if req.SellerID != "" {
		add("a.user_id = $%d", req.SellerID)
	}
	if req.PriceMin != nil {
		add("a.price_eur >= $%d", *req.PriceMin)
	}
	if req.PriceMax != nil {
		add("a.price_eur <= $%d", *req.PriceMax)
	}
	if req.ViewsMin != nil {
		add("a.views >= $%d", *req.ViewsMin)
	}
	if req.ViewsMax != nil {
		add("a.views <= $%d", *req.ViewsMax)
	}
	if req.PosterType != "" {
		add("a.poster_type = $%d", req.PosterType)
	}
	if req.IsActive != nil {
		add("a.is_active = $%d", *req.IsActive)
	}
	if req.IsDeleted != nil {
		add("a.is_deleted = $%d", *req.IsDeleted)
	} else {
		where = append(where, "a.is_deleted = false")
	}
	if req.DateFrom != "" {
		add("a.start_date >= $%d", req.DateFrom)
	}
	if req.DateTo != "" {
		dateTo := req.DateTo
		if len(dateTo) == 10 {
			dateTo += "T23:59:59.999+2359"
		}
		add("a.start_date <= $%d", dateTo)
	}
	if req.TaskID != "" {
		add("a.task_id = $%d", req.TaskID)
	}
	if req.HasImages != nil && *req.HasImages {
		where = append(where, "EXISTS (SELECT 1 FROM ad_images ai WHERE ai.ad_id = a.id)")
	}
	if req.FavoritesMin != nil {
		add("a.favorites >= $%d", *req.FavoritesMin)
	}
	if req.FavoritesMax != nil {
		add("a.favorites <= $%d", *req.FavoritesMax)
	}
	if req.ViewsDelta1hMin != nil {
		add("m.views_delta_1h >= $%d", *req.ViewsDelta1hMin)
	}
	if req.ViewsDelta1hMax != nil {
		add("m.views_delta_1h <= $%d", *req.ViewsDelta1hMax)
	}
	if req.ViewsDelta24hMin != nil {
		add("m.views_delta_24h >= $%d", *req.ViewsDelta24hMin)
	}
	if req.ViewsDelta24hMax != nil {
		add("m.views_delta_24h <= $%d", *req.ViewsDelta24hMax)
	}
	if req.ViewsPerHourMin != nil {
		add("m.views_per_hour >= $%d", *req.ViewsPerHourMin)
	}
	if req.FavoritesDelta1hMin != nil {
		add("COALESCE(m.favorites_delta_1h, 0) >= $%d", *req.FavoritesDelta1hMin)
	}
	if req.FavoritesDelta1hMax != nil {
		add("COALESCE(m.favorites_delta_1h, 0) <= $%d", *req.FavoritesDelta1hMax)
	}
	if req.FavoritesDelta24hMin != nil {
		add("COALESCE(m.favorites_delta_24h, 0) >= $%d", *req.FavoritesDelta24hMin)
	}
	if req.FavoritesDelta24hMax != nil {
		add("COALESCE(m.favorites_delta_24h, 0) <= $%d", *req.FavoritesDelta24hMax)
	}
	if req.FavoritesPerHourMin != nil {
		add("COALESCE(m.favorites_per_hour, 0) >= $%d", *req.FavoritesPerHourMin)
	}
	if req.AdType != "" {
		add("a.ad_type = $%d", req.AdType)
	}
	if req.PriceType != "" {
		add("a.price_type = $%d", req.PriceType)
	}
	if req.SellerAdsMin != nil {
		add("a.seller_ad_count >= $%d", *req.SellerAdsMin)
	}
	if req.SellerAdsMax != nil {
		add("a.seller_ad_count <= $%d", *req.SellerAdsMax)
	}
	if req.ShippingType != "" {
		add("a.shipping_type = $%d", req.ShippingType)
	}
	if req.ItemCondition != "" {
		add("a.item_condition = $%d", req.ItemCondition)
	}
	for key, val := range req.Attributes {
		safeKey := strings.Map(func(r rune) rune {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
				return r
			}
			return -1
		}, key)
		if safeKey != "" {
			where = append(where, fmt.Sprintf("a.attributes->>'%s' = $%d", safeKey, n))
			args = append(args, val)
			n++
		}
	}
	if req.DemandScoreMin != nil {
		add("COALESCE(a.demand_score, 0) >= $%d", *req.DemandScoreMin)
	}
	if req.FreshnessBoostMin != nil {
		where = append(where, fmt.Sprintf(`a.first_seen_at >= NOW() - INTERVAL '3 hours' AND LEAST(100,(COALESCE(m.views_per_hour,0)*5+COALESCE(a.favorites,0)*20)::numeric) >= $%d`, n))
		args = append(args, *req.FreshnessBoostMin)
		n++
	}
	if req.EngagementRateMin != nil {
		add("COALESCE(a.engagement_rate, 0) >= $%d", *req.EngagementRateMin)
	}
	if req.PhotoCountMin != nil {
		add("COALESCE(array_length(a.image_urls, 1), 0) >= $%d", *req.PhotoCountMin)
	}
	if req.QDescription != "" {
		add("a.description ILIKE $%d", "%"+req.QDescription+"%")
	}
	if req.ZipCode != "" {
		add("a.zip_code = $%d", req.ZipCode)
	}
	if req.PriceDropped != nil && *req.PriceDropped {
		where = append(where, "m.price_dropped = true")
	}

	metricsSorts := map[string]bool{
		"views_delta_1h": true, "views_delta_24h": true, "views_delta_7d": true,
		"views_per_hour": true,
	}
	if metricsSorts[req.SortBy] {
		where = append(where, "a.views > 0")
	}

	whereSQL := joinWhere(where)

	sortCol := "a.created_at"
	sortDir := "DESC"
	allowed := map[string]string{
		"price": "a.price_eur", "views": "a.views", "favorites": "a.favorites",
		"favorites_delta_1h": "COALESCE(m.favorites_delta_1h, 0)", "favorites_delta_24h": "COALESCE(m.favorites_delta_24h, 0)",
		"favorites_per_hour": "COALESCE(m.favorites_per_hour, 0)",
		"engagement_rate": "COALESCE(a.engagement_rate, 0)",
		"demand_score": "COALESCE(a.demand_score, 0)",
		"freshness_boost": "CASE WHEN a.first_seen_at>=NOW()-INTERVAL '3 hours' AND COALESCE(a.views,0)>0 THEN LEAST(100,(COALESCE(m.views_per_hour,0)*5+COALESCE(a.favorites,0)*20)::numeric) ELSE 0 END",
		"hours_to_sold": "CASE WHEN a.is_deleted AND a.deleted_at IS NOT NULL THEN EXTRACT(EPOCH FROM (a.deleted_at-a.first_seen_at))/3600.0 ELSE NULL END",
		"photo_count": "COALESCE(array_length(a.image_urls,1),0)",
		"created_at": "a.created_at", "updated_at": "a.updated_at",
		"start_date": "a.start_date", "first_seen": "a.first_seen_at",
		"views_delta_1h": "COALESCE(m.views_delta_1h, 0)", "views_delta_24h": "COALESCE(m.views_delta_24h, 0)",
		"views_delta_7d": "COALESCE(m.views_delta_7d, 0)", "views_per_hour": "COALESCE(m.views_per_hour, 0)",
		"snapshot_count": "COALESCE(m.snapshot_count, 0)",
	}
	if col, ok := allowed[req.SortBy]; ok {
		sortCol = col
	}
	if req.SortOrder == "asc" {
		sortDir = "ASC"
	}

	perPage := req.PerPage
	if perPage <= 0 {
		perPage = 50
	}
	if perPage > 200 {
		perPage = 200
	}
	page := req.Page
	if page < 1 {
		page = 1
	}
	offset := (page - 1) * perPage

	countArgs := make([]interface{}, len(args))
	copy(countArgs, args)

	var total int
	needsMetricsJoin := req.ViewsDelta1hMin != nil || req.ViewsDelta1hMax != nil || req.ViewsDelta24hMin != nil || req.ViewsDelta24hMax != nil || req.ViewsPerHourMin != nil || req.FavoritesDelta1hMin != nil || req.FavoritesDelta1hMax != nil || req.FavoritesDelta24hMin != nil || req.FavoritesDelta24hMax != nil || req.FavoritesPerHourMin != nil || req.FreshnessBoostMin != nil || (req.PriceDropped != nil && *req.PriceDropped)
	hasFilters := len(where) > 2 || len(args) > 0
	if !hasFilters {
		p.pool.QueryRow(ctx, "SELECT reltuples::bigint FROM pg_class WHERE relname = 'ads'").Scan(&total)
	} else if needsMetricsJoin {
		p.pool.QueryRow(ctx, "SELECT COUNT(*) FROM ads a LEFT JOIN ad_metrics m ON m.ad_id = a.id WHERE "+whereSQL, countArgs...).Scan(&total)
	} else {
		p.pool.QueryRow(ctx, "SELECT COUNT(*) FROM ads a WHERE "+whereSQL, countArgs...).Scan(&total)
	}

	if hasTextQuery && req.SortBy == "" {
		sortCol = "CASE WHEN a.title ILIKE $" + fmt.Sprintf("%d", n) + " THEN 0 ELSE 1 END, a.views"
		args = append(args, "%"+req.Query+"%")
		n++
		sortDir = "DESC"
	}

	dataSQL := fmt.Sprintf(`
		SELECT
			a.id, a.title, COALESCE(a.description, ''), a.price_eur, COALESCE(a.contact_name, ''),
			COALESCE(a.category_id, ''), COALESCE(a.ad_status, 'ACTIVE'), COALESCE(a.poster_type, ''), COALESCE(a.start_date, ''),
			COALESCE(a.url, ''), COALESCE(a.views, 0), COALESCE(a.favorites, 0), a.is_active, a.is_deleted,
			COALESCE(a.first_seen_at, a.created_at), a.created_at,
			COALESCE(a.location_id, ''), COALESCE(a.user_id, ''),
			COALESCE(a.ad_type, ''), COALESCE(a.price_type, ''),
			COALESCE(a.buy_now_selected, false), COALESCE(a.buy_now_price, 0), COALESCE(a.user_rating, 0),
			COALESCE(a.shipping_type, ''), COALESCE(a.item_condition, ''),
			COALESCE(a.zip_code, ''), COALESCE(a.city, ''),
			COALESCE(m.views_current, a.views, 0),
			COALESCE(m.favorites_current, a.favorites, 0),
			COALESCE(m.price_current, a.price_eur),
			COALESCE(m.views_delta_1h, 0),
			COALESCE(m.views_delta_24h, 0),
			COALESCE(m.views_delta_7d, 0),
			COALESCE(m.views_per_hour, 0),
			COALESCE(m.favorites_delta_1h, 0),
			COALESCE(m.favorites_delta_24h, 0),
			COALESCE(m.favorites_delta_7d, 0),
			COALESCE(m.favorites_per_hour, 0),
			COALESCE(m.price_previous, 0),
			COALESCE(m.price_min_seen, 0),
			COALESCE(m.price_max_seen, 0),
			COALESCE(m.price_dropped, false),
			COALESCE(m.price_change_pct, 0),
			COALESCE(m.snapshot_count, 0),
			COALESCE(m.first_seen_at, a.first_seen_at, a.created_at),
			COALESCE(m.last_snapshot_at, a.updated_at),
			COALESCE(
				(SELECT cdn_url FROM ad_images WHERE ad_id = a.id AND position = 0 LIMIT 1),
				a.image_urls[1]
			),
			COALESCE(a.engagement_rate, 0),
			COALESCE(a.demand_score, 0),
			CASE
				WHEN a.first_seen_at >= NOW() - INTERVAL '3 hours' AND COALESCE(a.views, 0) > 0 THEN
					LEAST(100, ROUND((COALESCE(m.views_per_hour, 0) * 5 + COALESCE(a.favorites, 0) * 20)::numeric, 1))
				ELSE 0
			END,
			CASE WHEN a.is_deleted AND a.deleted_at IS NOT NULL AND a.first_seen_at IS NOT NULL
				THEN ROUND(EXTRACT(EPOCH FROM (a.deleted_at - a.first_seen_at)) / 3600.0, 1)
				ELSE NULL END,
			COALESCE(array_length(a.image_urls, 1), 0),
			ROUND(EXTRACT(EPOCH FROM (NOW() - COALESCE(a.first_seen_at, a.created_at))) / 3600.0, 1),
			a.last_checked_at
		FROM ads a
		LEFT JOIN ad_metrics m ON m.ad_id = a.id
		WHERE %s
		ORDER BY %s %s NULLS LAST
		LIMIT $%d OFFSET $%d`, whereSQL, sortCol, sortDir, n, n+1)
	args = append(args, perPage, offset)

	rows, err := p.pool.Query(ctx, dataSQL, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var results []kl.AdWithMetrics
	for rows.Next() {
		var aw kl.AdWithMetrics
		var m kl.AdMetrics
		var thumbnail *string
		if err := rows.Scan(
			&aw.ID, &aw.Title, &aw.Description, &aw.PriceEUR, &aw.ContactName,
			&aw.CategoryID, &aw.AdStatus, &aw.PosterType, &aw.StartDate,
			&aw.URL, &aw.Views, &aw.Favorites, &aw.IsActive, &aw.IsDeleted, &aw.FirstSeenAt, &aw.CreatedAt,
			&aw.LocationID, &aw.UserID,
			&aw.AdType, &aw.PriceType, &aw.BuyNowSelected, &aw.BuyNowPrice, &aw.UserRating,
			&aw.ShippingType, &aw.ItemCondition,
			&aw.ZipCode, &aw.City,
			&m.ViewsCurrent, &m.FavoritesCurrent, &m.PriceCurrent,
			&m.ViewsDelta1h, &m.ViewsDelta24h, &m.ViewsDelta7d, &m.ViewsPerHour,
			&m.FavoritesDelta1h, &m.FavoritesDelta24h, &m.FavoritesDelta7d, &m.FavoritesPerHour,
			&m.PricePrevious, &m.PriceMinSeen, &m.PriceMaxSeen,
			&m.PriceDropped, &m.PriceChangePct,
			&m.SnapshotCount, &m.FirstSeenAt, &m.LastSnapshotAt,
			&thumbnail,
			&aw.EngagementRate, &aw.DemandScore, &aw.FreshnessBoost,
			&aw.HoursToSold, &aw.PhotoCount,
			&aw.HoursAlive, &aw.LastCheckedAt,
		); err != nil {
			log.Warn().Err(err).Str("ad_id", aw.ID).Msg("search scan error")
			continue
		}
		m.AdID = aw.ID
		aw.Metrics = &m
		if thumbnail != nil {
			aw.Thumbnail = *thumbnail
		}
		results = append(results, aw)
	}

	return results, total, nil
}

func (p *Postgres) GetDashboardStatsV2(ctx context.Context) (*kl.DashboardStats, error) {
	stats := &kl.DashboardStats{}
	batch := &pgx.Batch{}
	batch.Queue("SELECT COUNT(*) FROM ads")
	batch.Queue("SELECT COUNT(*) FROM ads WHERE is_active = true AND is_deleted = false")
	batch.Queue("SELECT COUNT(*) FROM ads WHERE is_deleted = true")
	batch.Queue("SELECT COUNT(*) FROM ad_images")
	batch.Queue("SELECT COUNT(*) FROM ads WHERE first_seen_at >= CURRENT_DATE")
	batch.Queue("SELECT COALESCE(AVG(price_eur), 0) FROM ads WHERE is_active = true AND price_eur > 0 AND price_eur < 50000")
	batch.Queue("SELECT COUNT(*) FROM ad_metrics WHERE views_delta_1h > 10")
	batch.Queue("SELECT COUNT(*) FROM ad_metrics WHERE price_dropped = true AND last_snapshot_at >= NOW() - INTERVAL '24 hours'")
	batch.Queue("SELECT COALESCE(SUM(snapshot_count), 0) FROM ad_metrics")
	batch.Queue(`SELECT c.id, c.name, COUNT(a.id)
		FROM categories c JOIN ads a ON a.category_id = c.id WHERE a.is_active = true
		GROUP BY c.id, c.name ORDER BY COUNT(a.id) DESC LIMIT 10`)

	results := p.pool.SendBatch(ctx, batch)
	defer results.Close()

	results.QueryRow().Scan(&stats.TotalAds)
	results.QueryRow().Scan(&stats.ActiveAds)
	results.QueryRow().Scan(&stats.DeletedAds)
	results.QueryRow().Scan(&stats.TotalImages)
	results.QueryRow().Scan(&stats.AdsToday)
	results.QueryRow().Scan(&stats.AvgPrice)
	results.QueryRow().Scan(&stats.TrendingAds)
	results.QueryRow().Scan(&stats.PriceDrops)
	results.QueryRow().Scan(&stats.TotalSnapshots)

	topRows, _ := results.Query()
	if topRows != nil {
		defer topRows.Close()
		for topRows.Next() {
			var cs kl.CategoryStat
			if topRows.Scan(&cs.CategoryID, &cs.Name, &cs.Count) == nil {
				stats.TopCategories = append(stats.TopCategories, cs)
			}
		}
	}
	return stats, nil
}

func splitSearchWords(q string) []string {
	parts := strings.Fields(strings.TrimSpace(q))
	var words []string
	for _, p := range parts {
		w := strings.TrimSpace(p)
		if len(w) >= 2 {
			words = append(words, w)
		}
	}
	if len(words) == 0 && q != "" {
		return []string{strings.TrimSpace(q)}
	}
	return words
}

func (p *Postgres) expandCategoryIDs(ctx context.Context, ids []string) []string {
	rows, err := p.pool.Query(ctx, `SELECT id, COALESCE(parent_id, '') FROM categories`)
	if err != nil {
		return ids
	}
	defer rows.Close()
	children := make(map[string][]string)
	for rows.Next() {
		var id, parentID string
		if rows.Scan(&id, &parentID) == nil && parentID != "" && parentID != "0" {
			children[parentID] = append(children[parentID], id)
		}
	}
	seen := make(map[string]bool)
	var result []string
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			result = append(result, id)
		}
		for _, child := range children[id] {
			if !seen[child] {
				seen[child] = true
				result = append(result, child)
			}
		}
	}
	log.Info().Int("input", len(ids)).Int("expanded", len(result)).Int("children_map", len(children)).Strs("ids", ids).Strs("result", result).Msg("expandCategoryIDs")
	if len(result) == 0 {
		return ids
	}
	return result
}

func joinWhere(parts []string) string {
	result := ""
	for i, p := range parts {
		if i > 0 {
			result += " AND "
		}
		result += p
	}
	return result
}
