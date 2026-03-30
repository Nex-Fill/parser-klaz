package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	kl "github.com/danamakarenko/klaz-parser/pkg/kleinanzeigen"
)

type Postgres struct {
	pool *pgxpool.Pool
}

func NewPostgres(ctx context.Context, dsn string, maxConns, minConns int) (*Postgres, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.MaxConns = int32(maxConns)
	cfg.MinConns = int32(minConns)
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

func (p *Postgres) Close()            { p.pool.Close() }
func (p *Postgres) Pool() *pgxpool.Pool { return p.pool }

// ==================== ADS ====================

func (p *Postgres) UpsertAd(ctx context.Context, ad *kl.Ad) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO ads (id, title, description, price, price_eur, contact_name,
			category_id, location_id, ad_status, shipping_option, user_id,
			user_since_date, poster_type, start_date, url, views, favorites, is_active,
			is_deleted, task_id, first_seen_at, created_at, updated_at, last_checked_at,
			ad_type, price_type, buy_now_selected, buy_now_price, user_rating, image_urls,
			shipping_type, item_condition)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32)
		ON CONFLICT (id) DO UPDATE SET
			title = EXCLUDED.title,
			description = EXCLUDED.description,
			price = EXCLUDED.price,
			price_eur = EXCLUDED.price_eur,
			contact_name = EXCLUDED.contact_name,
			category_id = EXCLUDED.category_id,
			location_id = EXCLUDED.location_id,
			ad_status = EXCLUDED.ad_status,
			shipping_option = EXCLUDED.shipping_option,
			user_id = CASE WHEN EXCLUDED.user_id != '' THEN EXCLUDED.user_id ELSE ads.user_id END,
			poster_type = EXCLUDED.poster_type,
			start_date = EXCLUDED.start_date,
			url = EXCLUDED.url,
			views = CASE WHEN EXCLUDED.views > 0 THEN EXCLUDED.views ELSE ads.views END,
			favorites = CASE WHEN EXCLUDED.favorites > 0 THEN EXCLUDED.favorites ELSE ads.favorites END,
			is_active = EXCLUDED.is_active,
			is_deleted = EXCLUDED.is_deleted,
			ad_type = CASE WHEN EXCLUDED.ad_type != '' THEN EXCLUDED.ad_type ELSE ads.ad_type END,
			price_type = CASE WHEN EXCLUDED.price_type != '' THEN EXCLUDED.price_type ELSE ads.price_type END,
			buy_now_selected = EXCLUDED.buy_now_selected,
			buy_now_price = CASE WHEN EXCLUDED.buy_now_price > 0 THEN EXCLUDED.buy_now_price ELSE ads.buy_now_price END,
			user_rating = CASE WHEN EXCLUDED.user_rating > 0 THEN EXCLUDED.user_rating ELSE ads.user_rating END,
			image_urls = CASE WHEN array_length(EXCLUDED.image_urls, 1) > 0 THEN EXCLUDED.image_urls ELSE ads.image_urls END,
			shipping_type = CASE WHEN EXCLUDED.shipping_type != '' THEN EXCLUDED.shipping_type ELSE ads.shipping_type END,
			item_condition = CASE WHEN EXCLUDED.item_condition != '' THEN EXCLUDED.item_condition ELSE ads.item_condition END,
			updated_at = EXCLUDED.updated_at,
			last_checked_at = EXCLUDED.last_checked_at`,
		ad.ID, ad.Title, ad.Description, ad.Price, ad.PriceEUR, ad.ContactName,
		ad.CategoryID, ad.LocationID, ad.AdStatus, ad.ShippingOption, ad.UserID,
		ad.UserSinceDate, ad.PosterType, ad.StartDate, ad.URL, ad.Views, ad.Favorites, ad.IsActive,
		ad.IsDeleted, ad.TaskID, ad.FirstSeenAt, ad.CreatedAt, ad.UpdatedAt, time.Now(),
		ad.AdType, ad.PriceType, ad.BuyNowSelected, ad.BuyNowPrice, ad.UserRating, ad.ImageURLs,
		ad.ShippingType, ad.ItemCondition,
	)
	return err
}

func (p *Postgres) UpsertAdsBatch(ctx context.Context, ads []*kl.Ad) error {
	batch := &pgx.Batch{}
	for _, ad := range ads {
		batch.Queue(`
			INSERT INTO ads (id, title, description, price, price_eur, contact_name,
				category_id, location_id, ad_status, shipping_option, user_id,
				user_since_date, poster_type, start_date, url, views, is_active,
				is_deleted, task_id, first_seen_at, created_at, updated_at, last_checked_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
			ON CONFLICT (id) DO UPDATE SET
				title = EXCLUDED.title, description = EXCLUDED.description,
				price = EXCLUDED.price, price_eur = EXCLUDED.price_eur,
				contact_name = EXCLUDED.contact_name, ad_status = EXCLUDED.ad_status,
				category_id = EXCLUDED.category_id, location_id = EXCLUDED.location_id,
				user_id = CASE WHEN EXCLUDED.user_id != '' THEN EXCLUDED.user_id ELSE ads.user_id END,
				poster_type = EXCLUDED.poster_type, start_date = EXCLUDED.start_date,
				url = EXCLUDED.url,
				views = CASE WHEN EXCLUDED.views > 0 THEN EXCLUDED.views ELSE ads.views END,
				is_active = EXCLUDED.is_active,
				updated_at = EXCLUDED.updated_at, last_checked_at = EXCLUDED.last_checked_at`,
			ad.ID, ad.Title, ad.Description, ad.Price, ad.PriceEUR, ad.ContactName,
			ad.CategoryID, ad.LocationID, ad.AdStatus, ad.ShippingOption, ad.UserID,
			ad.UserSinceDate, ad.PosterType, ad.StartDate, ad.URL, ad.Views, ad.IsActive,
			ad.IsDeleted, ad.TaskID, ad.FirstSeenAt, ad.CreatedAt, ad.UpdatedAt, time.Now(),
		)
	}
	results := p.pool.SendBatch(ctx, batch)
	defer results.Close()
	for range ads {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("batch exec: %w", err)
		}
	}
	return nil
}

func (p *Postgres) UpsertAdsFromSearch(ctx context.Context, ads []*kl.Ad) error {
	batch := &pgx.Batch{}
	for _, ad := range ads {
		batch.Queue(`
			INSERT INTO ads (id, title, description, price, price_eur, contact_name,
				category_id, location_id, ad_status, shipping_option, user_id,
				user_since_date, poster_type, start_date, url, views, is_active,
				is_deleted, task_id, first_seen_at, created_at, updated_at, last_checked_at,
				ad_type, price_type, image_urls)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,NULL,$23,$24,$25)
			ON CONFLICT (id) DO UPDATE SET
				title = EXCLUDED.title, description = EXCLUDED.description,
				price = EXCLUDED.price, price_eur = EXCLUDED.price_eur,
				ad_status = EXCLUDED.ad_status,
				user_id = CASE WHEN EXCLUDED.user_id != '' THEN EXCLUDED.user_id ELSE ads.user_id END,
				ad_type = CASE WHEN EXCLUDED.ad_type != '' THEN EXCLUDED.ad_type ELSE ads.ad_type END,
				price_type = CASE WHEN EXCLUDED.price_type != '' THEN EXCLUDED.price_type ELSE ads.price_type END,
				image_urls = CASE WHEN array_length(EXCLUDED.image_urls, 1) > 0 THEN EXCLUDED.image_urls ELSE ads.image_urls END,
				updated_at = EXCLUDED.updated_at`,
			ad.ID, ad.Title, ad.Description, ad.Price, ad.PriceEUR, ad.ContactName,
			ad.CategoryID, ad.LocationID, ad.AdStatus, ad.ShippingOption, ad.UserID,
			ad.UserSinceDate, ad.PosterType, ad.StartDate, ad.URL, ad.Views, ad.IsActive,
			ad.IsDeleted, ad.TaskID, ad.FirstSeenAt, ad.CreatedAt, ad.UpdatedAt,
			ad.AdType, ad.PriceType, ad.ImageURLs,
		)
	}
	results := p.pool.SendBatch(ctx, batch)
	defer results.Close()
	for range ads {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("batch exec: %w", err)
		}
	}
	return nil
}

func (p *Postgres) UpdateAdViews(ctx context.Context, adID string, views int) error {
	_, err := p.pool.Exec(ctx, `UPDATE ads SET views = $2, last_checked_at = NOW(), updated_at = NOW() WHERE id = $1`, adID, views)
	return err
}

func (p *Postgres) BatchUpdateCounters(ctx context.Context, views map[string]int, favorites map[string]int) error {
	batch := &pgx.Batch{}
	seen := make(map[string]bool)
	for id, v := range views {
		f := favorites[id]
		if v > 0 {
			batch.Queue(`UPDATE ads SET views = $2, favorites = $3, last_checked_at = NOW(), updated_at = NOW() WHERE id = $1`, id, v, f)
		} else {
			batch.Queue(`UPDATE ads SET favorites = $2, last_checked_at = NOW(), updated_at = NOW() WHERE id = $1`, id, f)
		}

		if v > 0 {
			batch.Queue(`INSERT INTO ad_metrics (ad_id, views_current, favorites_current, first_seen_at, last_snapshot_at, updated_at)
				VALUES ($1, $2, $3, NOW(), NOW(), NOW())
				ON CONFLICT (ad_id) DO UPDATE SET
					views_1h_ago = CASE WHEN ad_metrics.updated_at <= NOW() - INTERVAL '50 minutes' THEN ad_metrics.views_current ELSE ad_metrics.views_1h_ago END,
					views_24h_ago = CASE WHEN ad_metrics.updated_at <= NOW() - INTERVAL '23 hours' THEN ad_metrics.views_current ELSE ad_metrics.views_24h_ago END,
					views_7d_ago = CASE WHEN ad_metrics.updated_at <= NOW() - INTERVAL '6 days' THEN ad_metrics.views_current ELSE ad_metrics.views_7d_ago END,
					favorites_1h_ago = CASE WHEN ad_metrics.updated_at <= NOW() - INTERVAL '50 minutes' THEN ad_metrics.favorites_current ELSE ad_metrics.favorites_1h_ago END,
					favorites_24h_ago = CASE WHEN ad_metrics.updated_at <= NOW() - INTERVAL '23 hours' THEN ad_metrics.favorites_current ELSE ad_metrics.favorites_24h_ago END,
					favorites_7d_ago = CASE WHEN ad_metrics.updated_at <= NOW() - INTERVAL '6 days' THEN ad_metrics.favorites_current ELSE ad_metrics.favorites_7d_ago END,
					views_current = $2,
					favorites_current = $3,
					views_delta_1h = GREATEST($2 - COALESCE(ad_metrics.views_1h_ago, 0), 0),
					views_delta_24h = GREATEST($2 - COALESCE(ad_metrics.views_24h_ago, 0), 0),
					views_delta_7d = GREATEST($2 - COALESCE(ad_metrics.views_7d_ago, 0), 0),
					favorites_delta_1h = $3 - COALESCE(ad_metrics.favorites_1h_ago, 0),
					favorites_delta_24h = $3 - COALESCE(ad_metrics.favorites_24h_ago, 0),
					favorites_delta_7d = $3 - COALESCE(ad_metrics.favorites_7d_ago, 0),
					views_per_hour = CASE
						WHEN EXTRACT(EPOCH FROM (NOW() - ad_metrics.first_seen_at)) > 7200
						THEN GREATEST($2 - COALESCE(ad_metrics.views_7d_ago, 0), 0)::double precision / LEAST(GREATEST(EXTRACT(EPOCH FROM (NOW() - ad_metrics.first_seen_at)) / 3600.0, 1), 168)
						ELSE 0 END,
					favorites_per_hour = CASE
						WHEN EXTRACT(EPOCH FROM (NOW() - ad_metrics.first_seen_at)) > 7200
						THEN GREATEST($3 - COALESCE(ad_metrics.favorites_7d_ago, 0), 0)::double precision / LEAST(GREATEST(EXTRACT(EPOCH FROM (NOW() - ad_metrics.first_seen_at)) / 3600.0, 1), 168)
						ELSE 0 END,
					snapshot_count = ad_metrics.snapshot_count + 1,
					last_snapshot_at = NOW(),
					updated_at = NOW()`, id, v, f)
		} else if f >= 0 {
			batch.Queue(`UPDATE ad_metrics SET favorites_current = $2, updated_at = NOW() WHERE ad_id = $1`, id, f)
		}

		seen[id] = true
	}
	for id, f := range favorites {
		if !seen[id] {
			batch.Queue(`UPDATE ads SET favorites = $2, last_checked_at = NOW(), updated_at = NOW() WHERE id = $1`, id, f)
		}
	}
	if batch.Len() == 0 {
		return nil
	}
	results := p.pool.SendBatch(ctx, batch)
	defer results.Close()
	for i := 0; i < batch.Len(); i++ {
		if _, err := results.Exec(); err != nil {
			return err
		}
	}
	return nil
}

func (p *Postgres) CountExistingIDs(ctx context.Context, ids []string) (int, error) {
	var count int
	err := p.pool.QueryRow(ctx, `SELECT COUNT(*) FROM unnest($1::text[]) AS id WHERE EXISTS (SELECT 1 FROM ads WHERE ads.id = id)`, ids).Scan(&count)
	return count, err
}

func (p *Postgres) GetAllActiveAdIDs(ctx context.Context) ([]string, error) {
	rows, err := p.pool.Query(ctx, `SELECT id FROM ads WHERE is_active = true AND is_deleted = false AND (last_checked_at < NOW() - INTERVAL '40 minutes' OR last_checked_at IS NULL) ORDER BY last_checked_at ASC NULLS FIRST`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func (p *Postgres) RefreshSellerAdCounts(ctx context.Context) {
	_, err := p.pool.Exec(ctx, `
		UPDATE ads SET seller_ad_count = sub.cnt
		FROM (SELECT user_id, COUNT(*) as cnt FROM ads WHERE is_active = true AND user_id != '' GROUP BY user_id) sub
		WHERE ads.user_id = sub.user_id AND ads.is_active = true AND ads.user_id != ''`)
	if err != nil {
		log.Error().Err(err).Msg("refresh seller ad counts failed")
	} else {
		log.Info().Msg("seller ad counts refreshed")
	}
}

func (p *Postgres) SetStatusChangedAt(ctx context.Context, adID string, at time.Time) {
	p.pool.Exec(ctx, `UPDATE ads SET status_changed_at = $2 WHERE id = $1`, adID, at)
}

func (p *Postgres) MarkAdDeleted(ctx context.Context, adID string) error {
	now := time.Now()
	_, err := p.pool.Exec(ctx, `
		UPDATE ads SET is_active = false, is_deleted = true, deleted_at = $2,
			ad_status = 'DELETED', updated_at = $2, last_checked_at = $2
		WHERE id = $1`, adID, now)
	return err
}

func (p *Postgres) GetAd(ctx context.Context, id string) (*kl.Ad, error) {
	var ad kl.Ad
	err := p.pool.QueryRow(ctx, `
		SELECT id, title, description, price, price_eur, contact_name,
			category_id, location_id, ad_status, shipping_option, user_id,
			user_since_date, poster_type, start_date, url, views, COALESCE(favorites, 0), is_active,
			is_deleted, deleted_at, task_id, first_seen_at, created_at, updated_at, last_checked_at,
			COALESCE(ad_type, ''), COALESCE(price_type, ''), COALESCE(buy_now_selected, false),
			COALESCE(buy_now_price, 0), COALESCE(user_rating, 0), COALESCE(image_urls, ARRAY[]::TEXT[]),
			COALESCE(shipping_type, ''), COALESCE(item_condition, '')
		FROM ads WHERE id = $1`, id,
	).Scan(
		&ad.ID, &ad.Title, &ad.Description, &ad.Price, &ad.PriceEUR, &ad.ContactName,
		&ad.CategoryID, &ad.LocationID, &ad.AdStatus, &ad.ShippingOption, &ad.UserID,
		&ad.UserSinceDate, &ad.PosterType, &ad.StartDate, &ad.URL, &ad.Views, &ad.Favorites, &ad.IsActive,
		&ad.IsDeleted, &ad.DeletedAt, &ad.TaskID, &ad.FirstSeenAt, &ad.CreatedAt, &ad.UpdatedAt, &ad.LastCheckedAt,
		&ad.AdType, &ad.PriceType, &ad.BuyNowSelected, &ad.BuyNowPrice, &ad.UserRating, &ad.ImageURLs,
		&ad.ShippingType, &ad.ItemCondition,
	)
	if err != nil {
		return nil, err
	}
	return &ad, nil
}

func (p *Postgres) GetAdWithImages(ctx context.Context, id string) (*kl.Ad, error) {
	ad, err := p.GetAd(ctx, id)
	if err != nil {
		return nil, err
	}

	rows, err := p.pool.Query(ctx, `
		SELECT ad_id, position, original_url, s3_key, preview_key, hash, extension, cdn_url
		FROM ad_images WHERE ad_id = $1 ORDER BY position`, id)
	if err != nil {
		return ad, nil
	}
	defer rows.Close()

	for rows.Next() {
		var img kl.AdImage
		if err := rows.Scan(&img.AdID, &img.Position, &img.OriginalURL, &img.S3Key,
			&img.PreviewKey, &img.Hash, &img.Extension, &img.CDNUrl); err != nil {
			continue
		}
		ad.Images = append(ad.Images, img)
	}
	return ad, nil
}

func (p *Postgres) QueryAdIDs(ctx context.Context, query string, args ...interface{}) ([]string, error) {
	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func (p *Postgres) GetAdsWithoutImages(ctx context.Context, limit int) ([]string, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT a.id FROM ads a
		WHERE a.is_active = true AND a.is_deleted = false
			AND NOT EXISTS (SELECT 1 FROM ad_images ai WHERE ai.ad_id = a.id)
		ORDER BY a.first_seen_at DESC
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func (p *Postgres) GetAdCount(ctx context.Context) (int64, error) {
	var count int64
	err := p.pool.QueryRow(ctx, "SELECT COUNT(*) FROM ads WHERE is_active = true").Scan(&count)
	return count, err
}

// ==================== IMAGES ====================

func (p *Postgres) UpsertImages(ctx context.Context, images []kl.AdImage) error {
	batch := &pgx.Batch{}
	for _, img := range images {
		batch.Queue(`
			INSERT INTO ad_images (ad_id, position, original_url, s3_key, preview_key, hash, extension, cdn_url)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
			ON CONFLICT (ad_id, position) DO UPDATE SET
				s3_key = EXCLUDED.s3_key, preview_key = EXCLUDED.preview_key,
				hash = EXCLUDED.hash, cdn_url = EXCLUDED.cdn_url`,
			img.AdID, img.Position, img.OriginalURL, img.S3Key, img.PreviewKey, img.Hash, img.Extension, img.CDNUrl)
	}
	results := p.pool.SendBatch(ctx, batch)
	defer results.Close()
	for range images {
		if _, err := results.Exec(); err != nil {
			log.Warn().Err(err).Msg("image batch exec")
		}
	}
	return nil
}

// ==================== HISTORY ====================

func (p *Postgres) RecordHistory(ctx context.Context, adID, field, oldVal, newVal string) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO ad_history (ad_id, field_name, old_value, new_value, changed_at)
		VALUES ($1, $2, $3, $4, $5)`, adID, field, oldVal, newVal, time.Now())
	return err
}

func (p *Postgres) GetAdHistory(ctx context.Context, adID string, limit int) ([]kl.AdHistory, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := p.pool.Query(ctx, `
		SELECT id, ad_id, field_name, old_value, new_value, changed_at
		FROM ad_history WHERE ad_id = $1
		ORDER BY changed_at DESC LIMIT $2`, adID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var history []kl.AdHistory
	for rows.Next() {
		var h kl.AdHistory
		if rows.Scan(&h.ID, &h.AdID, &h.Field, &h.OldValue, &h.NewValue, &h.ChangedAt) == nil {
			history = append(history, h)
		}
	}
	return history, nil
}

// ==================== TASKS ====================

func (p *Postgres) SaveTask(ctx context.Context, task *kl.ParseTask) error {
	catJSON, _ := json.Marshal(task.CategoryURLs)
	filterJSON, _ := json.Marshal(task.Filters)
	progressJSON, _ := json.Marshal(task.Progress)

	_, err := p.pool.Exec(ctx, `
		INSERT INTO parse_tasks (task_id, task_name, status, category_urls, filters,
			max_pages_per_category, max_ads_to_check, monitor_hours, user_id,
			progress, created_at, updated_at, completed_at, error, ads_count)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
		ON CONFLICT (task_id) DO UPDATE SET
			status = EXCLUDED.status, progress = EXCLUDED.progress,
			updated_at = EXCLUDED.updated_at, completed_at = EXCLUDED.completed_at,
			error = EXCLUDED.error, ads_count = EXCLUDED.ads_count`,
		task.ID, task.Name, task.Status, catJSON, filterJSON,
		task.MaxPagesPerCategory, task.MaxAdsToCheck, task.MonitorHours, task.UserID,
		progressJSON, task.CreatedAt, task.UpdatedAt, task.CompletedAt, task.Error, task.AdsCount)
	return err
}

func (p *Postgres) GetAdsByTask(ctx context.Context, taskID string, offset, limit int) ([]*kl.Ad, int, error) {
	var total int
	p.pool.QueryRow(ctx, "SELECT COUNT(*) FROM ads WHERE task_id = $1", taskID).Scan(&total)

	rows, err := p.pool.Query(ctx, `
		SELECT id, title, price_eur, category_id, ad_status, url, views, start_date, is_deleted, created_at
		FROM ads WHERE task_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3`, taskID, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var ads []*kl.Ad
	for rows.Next() {
		var ad kl.Ad
		if rows.Scan(&ad.ID, &ad.Title, &ad.PriceEUR, &ad.CategoryID, &ad.AdStatus,
			&ad.URL, &ad.Views, &ad.StartDate, &ad.IsDeleted, &ad.CreatedAt) == nil {
			ads = append(ads, &ad)
		}
	}
	return ads, total, nil
}

// ==================== CATEGORIES ====================

func (p *Postgres) UpsertCategory(ctx context.Context, cat kl.CategoryNode) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO categories (id, name, parent_id, has_children, level, synced_at)
		VALUES ($1, $2, NULLIF($3, ''), $4, $5, NOW())
		ON CONFLICT (id) DO UPDATE SET
			name = EXCLUDED.name, parent_id = EXCLUDED.parent_id,
			has_children = EXCLUDED.has_children, level = EXCLUDED.level,
			synced_at = NOW()`,
		cat.ID, cat.Name, cat.ParentID, cat.HasChildren, cat.Level)
	return err
}

func (p *Postgres) UpsertCategoriesBatch(ctx context.Context, cats []kl.CategoryNode) error {
	sort.Slice(cats, func(i, j int) bool { return cats[i].Level < cats[j].Level })

	batch := &pgx.Batch{}
	for _, cat := range cats {
		batch.Queue(`
			INSERT INTO categories (id, name, parent_id, has_children, level, synced_at)
			VALUES ($1, $2, NULLIF($3, ''), $4, $5, NOW())
			ON CONFLICT (id) DO UPDATE SET
				name = EXCLUDED.name, has_children = EXCLUDED.has_children, synced_at = NOW()`,
			cat.ID, cat.Name, cat.ParentID, cat.HasChildren, cat.Level)
	}
	results := p.pool.SendBatch(ctx, batch)
	defer results.Close()
	for range cats {
		if _, err := results.Exec(); err != nil {
			log.Warn().Err(err).Msg("category upsert error")
		}
	}
	return nil
}

func (p *Postgres) GetCategories(ctx context.Context, parentID string) ([]kl.CategoryNode, error) {
	var rows pgx.Rows
	var err error
	if parentID == "" {
		rows, err = p.pool.Query(ctx, `
			SELECT id, name, COALESCE(parent_id, ''), has_children, level
			FROM categories WHERE parent_id IS NULL ORDER BY name`)
	} else {
		rows, err = p.pool.Query(ctx, `
			SELECT id, name, COALESCE(parent_id, ''), has_children, level
			FROM categories WHERE parent_id = $1 ORDER BY name`, parentID)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cats []kl.CategoryNode
	for rows.Next() {
		var c kl.CategoryNode
		if rows.Scan(&c.ID, &c.Name, &c.ParentID, &c.HasChildren, &c.Level) == nil {
			cats = append(cats, c)
		}
	}
	return cats, nil
}

func (p *Postgres) GetAllCategories(ctx context.Context) ([]kl.CategoryNode, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT id, name, COALESCE(parent_id, ''), has_children, level
		FROM categories ORDER BY level, name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cats []kl.CategoryNode
	for rows.Next() {
		var c kl.CategoryNode
		if rows.Scan(&c.ID, &c.Name, &c.ParentID, &c.HasChildren, &c.Level) == nil {
			cats = append(cats, c)
		}
	}
	return cats, nil
}

func (p *Postgres) SearchCategories(ctx context.Context, query string) ([]kl.CategoryNode, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT id, name, COALESCE(parent_id, ''), has_children, level
		FROM categories WHERE name ILIKE $1 ORDER BY level, name LIMIT 50`,
		"%"+query+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cats []kl.CategoryNode
	for rows.Next() {
		var c kl.CategoryNode
		if rows.Scan(&c.ID, &c.Name, &c.ParentID, &c.HasChildren, &c.Level) == nil {
			cats = append(cats, c)
		}
	}
	return cats, nil
}

// ==================== USERS ====================

func (p *Postgres) CreateUser(ctx context.Context, user *kl.User) error {
	if user.Role == "" {
		user.Role = "free"
	}
	_, err := p.pool.Exec(ctx, `
		INSERT INTO users (id, email, password_hash, name, role, is_admin, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		user.ID, user.Email, user.PasswordHash, user.Name, user.Role, user.IsAdmin, user.CreatedAt)
	return err
}

func (p *Postgres) GetUserByEmail(ctx context.Context, email string) (*kl.User, error) {
	var u kl.User
	err := p.pool.QueryRow(ctx, `
		SELECT id, email, password_hash, name, COALESCE(role,'free'), is_admin, created_at, COALESCE(last_login_at, created_at)
		FROM users WHERE email = $1`, email,
	).Scan(&u.ID, &u.Email, &u.PasswordHash, &u.Name, &u.Role, &u.IsAdmin, &u.CreatedAt, &u.LastLoginAt)
	if err != nil {
		return nil, err
	}
	return &u, nil
}

func (p *Postgres) GetUserByID(ctx context.Context, id string) (*kl.User, error) {
	var u kl.User
	err := p.pool.QueryRow(ctx, `
		SELECT id, email, password_hash, name, COALESCE(role,'free'), is_admin, created_at, COALESCE(last_login_at, created_at)
		FROM users WHERE id = $1`, id,
	).Scan(&u.ID, &u.Email, &u.PasswordHash, &u.Name, &u.Role, &u.IsAdmin, &u.CreatedAt, &u.LastLoginAt)
	if err != nil {
		return nil, err
	}
	return &u, nil
}

func (p *Postgres) UpdateLastLogin(ctx context.Context, userID string) {
	p.pool.Exec(ctx, "UPDATE users SET last_login_at = NOW() WHERE id = $1", userID)
}

// ==================== SAVED FILTERS ====================

func (p *Postgres) SaveFilter(ctx context.Context, sf *kl.SavedFilter) error {
	filtersJSON, _ := json.Marshal(sf.Filters)
	_, err := p.pool.Exec(ctx, `
		INSERT INTO saved_filters (id, user_id, name, filters, category_ids, notify_on_new, notify_on_price_drop, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (id) DO UPDATE SET
			name = EXCLUDED.name, filters = EXCLUDED.filters, category_ids = EXCLUDED.category_ids,
			notify_on_new = EXCLUDED.notify_on_new, notify_on_price_drop = EXCLUDED.notify_on_price_drop,
			updated_at = EXCLUDED.updated_at`,
		sf.ID, sf.UserID, sf.Name, filtersJSON, sf.CategoryIDs,
		sf.NotifyOnNew, sf.NotifyOnPriceDrop, sf.CreatedAt, time.Now())
	return err
}

func (p *Postgres) GetSavedFilters(ctx context.Context, userID string) ([]kl.SavedFilter, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT id, user_id, name, filters, category_ids, notify_on_new, notify_on_price_drop, created_at, updated_at
		FROM saved_filters WHERE user_id = $1 ORDER BY created_at DESC`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var filters []kl.SavedFilter
	for rows.Next() {
		var sf kl.SavedFilter
		var filtersJSON []byte
		if rows.Scan(&sf.ID, &sf.UserID, &sf.Name, &filtersJSON, &sf.CategoryIDs,
			&sf.NotifyOnNew, &sf.NotifyOnPriceDrop, &sf.CreatedAt, &sf.UpdatedAt) == nil {
			json.Unmarshal(filtersJSON, &sf.Filters)
			filters = append(filters, sf)
		}
	}
	return filters, nil
}

func (p *Postgres) DeleteSavedFilter(ctx context.Context, filterID, userID string) error {
	_, err := p.pool.Exec(ctx, "DELETE FROM saved_filters WHERE id = $1 AND user_id = $2", filterID, userID)
	return err
}

// ==================== SAVED FILTER NOTIFICATIONS ====================

func (p *Postgres) GetActiveNotifyFilters(ctx context.Context) ([]kl.SavedFilter, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT id, user_id, name, filters, category_ids, notify_on_new, notify_on_price_drop, created_at, updated_at
		FROM saved_filters
		WHERE notify_on_new = true OR notify_on_price_drop = true
		ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var filters []kl.SavedFilter
	for rows.Next() {
		var sf kl.SavedFilter
		var filtersJSON []byte
		if rows.Scan(&sf.ID, &sf.UserID, &sf.Name, &filtersJSON, &sf.CategoryIDs,
			&sf.NotifyOnNew, &sf.NotifyOnPriceDrop, &sf.CreatedAt, &sf.UpdatedAt) == nil {
			json.Unmarshal(filtersJSON, &sf.Filters)
			filters = append(filters, sf)
		}
	}
	return filters, nil
}

func (p *Postgres) GetNewAdsSince(ctx context.Context, categoryIDs []string, since time.Time) ([]kl.Ad, error) {
	query := `SELECT id, title, price_eur, category_id, url
		FROM ads WHERE first_seen_at >= $1 AND is_active = true AND is_deleted = false`
	args := []interface{}{since}
	if len(categoryIDs) > 0 {
		query += " AND category_id = ANY($2)"
		args = append(args, categoryIDs)
	}
	query += " ORDER BY first_seen_at DESC LIMIT 100"

	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ads []kl.Ad
	for rows.Next() {
		var ad kl.Ad
		if rows.Scan(&ad.ID, &ad.Title, &ad.PriceEUR, &ad.CategoryID, &ad.URL) == nil {
			ads = append(ads, ad)
		}
	}
	return ads, nil
}

func (p *Postgres) GetRecentPriceDrops(ctx context.Context, categoryIDs []string, since time.Time) ([]kl.Ad, error) {
	query := `SELECT a.id, a.title, a.price_eur, a.category_id, a.url
		FROM ads a
		JOIN ad_metrics m ON m.ad_id = a.id
		WHERE m.price_dropped = true AND m.last_snapshot_at >= $1
			AND a.is_active = true AND a.is_deleted = false`
	args := []interface{}{since}
	if len(categoryIDs) > 0 {
		query += " AND a.category_id = ANY($2)"
		args = append(args, categoryIDs)
	}
	query += " ORDER BY m.last_snapshot_at DESC LIMIT 100"

	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ads []kl.Ad
	for rows.Next() {
		var ad kl.Ad
		if rows.Scan(&ad.ID, &ad.Title, &ad.PriceEUR, &ad.CategoryID, &ad.URL) == nil {
			ads = append(ads, ad)
		}
	}
	return ads, nil
}

// ==================== NOTIFICATIONS ====================

func (p *Postgres) GetNotifications(ctx context.Context, userID string, onlyUnread bool, limit int) ([]kl.Notification, error) {
	if limit <= 0 {
		limit = 50
	}
	query := `SELECT id, user_id, type, title, COALESCE(body, ''), COALESCE(data, '{}'), is_read, created_at
		FROM notifications WHERE user_id = $1`
	if onlyUnread {
		query += " AND is_read = false"
	}
	query += " ORDER BY created_at DESC LIMIT $2"

	rows, err := p.pool.Query(ctx, query, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var notifs []kl.Notification
	for rows.Next() {
		var n kl.Notification
		var dataJSON []byte
		if rows.Scan(&n.ID, &n.UserID, &n.Type, &n.Title, &n.Body, &dataJSON, &n.IsRead, &n.CreatedAt) == nil {
			json.Unmarshal(dataJSON, &n.Data)
			notifs = append(notifs, n)
		}
	}
	return notifs, nil
}

func (p *Postgres) GetUnreadNotificationCount(ctx context.Context, userID string) (int, error) {
	var count int
	err := p.pool.QueryRow(ctx, "SELECT COUNT(*) FROM notifications WHERE user_id = $1 AND is_read = false", userID).Scan(&count)
	return count, err
}

func (p *Postgres) MarkNotificationRead(ctx context.Context, notifID int64, userID string) error {
	_, err := p.pool.Exec(ctx, "UPDATE notifications SET is_read = true WHERE id = $1 AND user_id = $2", notifID, userID)
	return err
}

func (p *Postgres) MarkAllNotificationsRead(ctx context.Context, userID string) error {
	_, err := p.pool.Exec(ctx, "UPDATE notifications SET is_read = true WHERE user_id = $1 AND is_read = false", userID)
	return err
}

func (p *Postgres) CreateNotification(ctx context.Context, n *kl.Notification) error {
	dataJSON, _ := json.Marshal(n.Data)
	return p.pool.QueryRow(ctx, `
		INSERT INTO notifications (user_id, type, title, body, data, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id`,
		n.UserID, n.Type, n.Title, n.Body, dataJSON, time.Now(),
	).Scan(&n.ID)
}

// ==================== BATCH ADS ====================

func (p *Postgres) GetAdsBatch(ctx context.Context, ids []string) ([]kl.AdWithMetrics, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT
			a.id, a.title, COALESCE(a.description, ''), a.price_eur, COALESCE(a.contact_name, ''),
			COALESCE(a.category_id, ''), COALESCE(a.location_id, ''), COALESCE(a.ad_status, 'ACTIVE'),
			COALESCE(a.poster_type, ''), COALESCE(a.start_date, ''),
			COALESCE(a.url, ''), COALESCE(a.views, 0), COALESCE(a.favorites, 0),
			a.is_active, a.is_deleted,
			COALESCE(a.first_seen_at, a.created_at), a.created_at,
			COALESCE(a.user_id, ''), COALESCE(a.user_since_date, ''),
			COALESCE(m.views_current, a.views, 0),
			COALESCE(m.price_current, a.price_eur),
			COALESCE(m.views_delta_1h, 0), COALESCE(m.views_delta_24h, 0), COALESCE(m.views_delta_7d, 0),
			COALESCE(m.views_per_hour, 0),
			COALESCE(m.price_previous, 0), COALESCE(m.price_min_seen, 0), COALESCE(m.price_max_seen, 0),
			COALESCE(m.price_dropped, false), COALESCE(m.price_change_pct, 0),
			COALESCE(m.snapshot_count, 0),
			COALESCE(m.first_seen_at, a.first_seen_at, a.created_at),
			COALESCE(m.last_snapshot_at, a.updated_at),
			(SELECT cdn_url FROM ad_images WHERE ad_id = a.id AND position = 0 LIMIT 1)
		FROM ads a
		LEFT JOIN ad_metrics m ON m.ad_id = a.id
		WHERE a.id = ANY($1)`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []kl.AdWithMetrics
	for rows.Next() {
		var aw kl.AdWithMetrics
		var met kl.AdMetrics
		var thumbnail *string
		if err := rows.Scan(
			&aw.ID, &aw.Title, &aw.Description, &aw.PriceEUR, &aw.ContactName,
			&aw.CategoryID, &aw.LocationID, &aw.AdStatus,
			&aw.PosterType, &aw.StartDate,
			&aw.URL, &aw.Views, &aw.Favorites,
			&aw.IsActive, &aw.IsDeleted,
			&aw.FirstSeenAt, &aw.CreatedAt,
			&aw.UserID, &aw.UserSinceDate,
			&met.ViewsCurrent, &met.PriceCurrent,
			&met.ViewsDelta1h, &met.ViewsDelta24h, &met.ViewsDelta7d,
			&met.ViewsPerHour,
			&met.PricePrevious, &met.PriceMinSeen, &met.PriceMaxSeen,
			&met.PriceDropped, &met.PriceChangePct,
			&met.SnapshotCount, &met.FirstSeenAt, &met.LastSnapshotAt,
			&thumbnail,
		); err != nil {
			continue
		}
		met.AdID = aw.ID
		aw.Metrics = &met
		if thumbnail != nil {
			aw.Thumbnail = *thumbnail
		}
		results = append(results, aw)
	}

	for i := range results {
		imgs, err := p.getAdImages(ctx, results[i].ID)
		if err == nil {
			results[i].Images = imgs
		}
	}

	return results, nil
}

func (p *Postgres) getAdImages(ctx context.Context, adID string) ([]kl.AdImage, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT ad_id, position, original_url, s3_key, preview_key, hash, extension, cdn_url
		FROM ad_images WHERE ad_id = $1 ORDER BY position`, adID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var imgs []kl.AdImage
	for rows.Next() {
		var img kl.AdImage
		if rows.Scan(&img.AdID, &img.Position, &img.OriginalURL, &img.S3Key,
			&img.PreviewKey, &img.Hash, &img.Extension, &img.CDNUrl) == nil {
			imgs = append(imgs, img)
		}
	}
	return imgs, nil
}

// ==================== SELLER ====================

func (p *Postgres) GetSellerProfile(ctx context.Context, sellerID string) (*kl.SellerProfile, error) {
	sp := &kl.SellerProfile{UserID: sellerID}
	err := p.pool.QueryRow(ctx, `
		SELECT
			COALESCE(MAX(contact_name), ''),
			COALESCE(MAX(user_since_date), ''),
			COUNT(*),
			COUNT(*) FILTER (WHERE is_active = true AND is_deleted = false)
		FROM ads WHERE user_id = $1`, sellerID,
	).Scan(&sp.Name, &sp.SinceDate, &sp.TotalAds, &sp.ActiveAds)
	if err != nil {
		return nil, err
	}
	return sp, nil
}

func (p *Postgres) GetSellerAds(ctx context.Context, sellerID string, offset, limit int) ([]kl.AdWithMetrics, int, error) {
	var total int
	p.pool.QueryRow(ctx, "SELECT COUNT(*) FROM ads WHERE user_id = $1 AND is_deleted = false", sellerID).Scan(&total)

	rows, err := p.pool.Query(ctx, `
		SELECT
			a.id, a.title, COALESCE(a.description, ''), a.price_eur, COALESCE(a.contact_name, ''),
			COALESCE(a.category_id, ''), COALESCE(a.location_id, ''), COALESCE(a.ad_status, 'ACTIVE'),
			COALESCE(a.poster_type, ''), COALESCE(a.start_date, ''),
			COALESCE(a.url, ''), COALESCE(a.views, 0), COALESCE(a.favorites, 0),
			a.is_active, a.is_deleted,
			COALESCE(a.first_seen_at, a.created_at), a.created_at,
			COALESCE(m.views_current, a.views, 0), COALESCE(m.price_current, a.price_eur),
			COALESCE(m.views_delta_1h, 0), COALESCE(m.views_delta_24h, 0),
			COALESCE(m.views_per_hour, 0),
			COALESCE(m.price_dropped, false), COALESCE(m.price_change_pct, 0),
			(SELECT cdn_url FROM ad_images WHERE ad_id = a.id AND position = 0 LIMIT 1)
		FROM ads a
		LEFT JOIN ad_metrics m ON m.ad_id = a.id
		WHERE a.user_id = $1 AND a.is_deleted = false
		ORDER BY a.created_at DESC
		LIMIT $2 OFFSET $3`, sellerID, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var results []kl.AdWithMetrics
	for rows.Next() {
		var aw kl.AdWithMetrics
		var met kl.AdMetrics
		var thumbnail *string
		if err := rows.Scan(
			&aw.ID, &aw.Title, &aw.Description, &aw.PriceEUR, &aw.ContactName,
			&aw.CategoryID, &aw.LocationID, &aw.AdStatus,
			&aw.PosterType, &aw.StartDate,
			&aw.URL, &aw.Views, &aw.Favorites,
			&aw.IsActive, &aw.IsDeleted,
			&aw.FirstSeenAt, &aw.CreatedAt,
			&met.ViewsCurrent, &met.PriceCurrent,
			&met.ViewsDelta1h, &met.ViewsDelta24h,
			&met.ViewsPerHour,
			&met.PriceDropped, &met.PriceChangePct,
			&thumbnail,
		); err != nil {
			continue
		}
		met.AdID = aw.ID
		aw.Metrics = &met
		if thumbnail != nil {
			aw.Thumbnail = *thumbnail
		}
		results = append(results, aw)
	}
	return results, total, nil
}
