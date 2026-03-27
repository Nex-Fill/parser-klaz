package storage

import (
	"context"
	"encoding/json"
	"fmt"
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
			user_since_date, poster_type, start_date, url, views, is_active,
			is_deleted, task_id, first_seen_at, created_at, updated_at, last_checked_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
		ON CONFLICT (id) DO UPDATE SET
			title = EXCLUDED.title,
			description = EXCLUDED.description,
			price = EXCLUDED.price,
			price_eur = EXCLUDED.price_eur,
			contact_name = EXCLUDED.contact_name,
			ad_status = EXCLUDED.ad_status,
			shipping_option = EXCLUDED.shipping_option,
			views = EXCLUDED.views,
			is_active = EXCLUDED.is_active,
			is_deleted = EXCLUDED.is_deleted,
			updated_at = EXCLUDED.updated_at,
			last_checked_at = EXCLUDED.last_checked_at`,
		ad.ID, ad.Title, ad.Description, ad.Price, ad.PriceEUR, ad.ContactName,
		ad.CategoryID, ad.LocationID, ad.AdStatus, ad.ShippingOption, ad.UserID,
		ad.UserSinceDate, ad.PosterType, ad.StartDate, ad.URL, ad.Views, ad.IsActive,
		ad.IsDeleted, ad.TaskID, ad.FirstSeenAt, ad.CreatedAt, ad.UpdatedAt, time.Now(),
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
				views = EXCLUDED.views, is_active = EXCLUDED.is_active,
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
			user_since_date, poster_type, start_date, url, views, is_active,
			is_deleted, deleted_at, task_id, first_seen_at, created_at, updated_at, last_checked_at
		FROM ads WHERE id = $1`, id,
	).Scan(
		&ad.ID, &ad.Title, &ad.Description, &ad.Price, &ad.PriceEUR, &ad.ContactName,
		&ad.CategoryID, &ad.LocationID, &ad.AdStatus, &ad.ShippingOption, &ad.UserID,
		&ad.UserSinceDate, &ad.PosterType, &ad.StartDate, &ad.URL, &ad.Views, &ad.IsActive,
		&ad.IsDeleted, &ad.DeletedAt, &ad.TaskID, &ad.FirstSeenAt, &ad.CreatedAt, &ad.UpdatedAt, &ad.LastCheckedAt,
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
		results.Exec()
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
