-- +goose Up

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ==================== USERS ====================

CREATE TABLE users (
    id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    name TEXT DEFAULT '',
    is_admin BOOLEAN DEFAULT false,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_login_at TIMESTAMPTZ
);

CREATE INDEX idx_users_email ON users(email);

-- ==================== CATEGORIES ====================

CREATE TABLE categories (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    parent_id TEXT,
    has_children BOOLEAN DEFAULT false,
    level INTEGER DEFAULT 0,
    ad_count INTEGER DEFAULT 0,
    synced_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (parent_id) REFERENCES categories(id) ON DELETE SET NULL
);

CREATE INDEX idx_cat_parent ON categories(parent_id);
CREATE INDEX idx_cat_level ON categories(level);

-- ==================== ADS ====================

CREATE TABLE ads (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT DEFAULT '',
    price BIGINT DEFAULT 0,
    price_eur DOUBLE PRECISION DEFAULT 0,
    contact_name TEXT DEFAULT '',
    category_id TEXT DEFAULT '',
    location_id TEXT DEFAULT '',
    ad_status TEXT DEFAULT 'ACTIVE',
    shipping_option TEXT DEFAULT '',
    user_id TEXT DEFAULT '',
    user_since_date TEXT DEFAULT '',
    poster_type TEXT DEFAULT '',
    start_date TEXT DEFAULT '',
    last_edit_date TEXT DEFAULT '',
    seller_account_type TEXT DEFAULT '',
    buy_now_enabled BOOLEAN DEFAULT false,
    url TEXT DEFAULT '',
    views INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT true,
    is_deleted BOOLEAN DEFAULT false,
    deleted_at TIMESTAMPTZ,
    task_id TEXT DEFAULT '',
    first_seen_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    last_checked_at TIMESTAMPTZ
);

CREATE INDEX idx_ads_category ON ads(category_id);
CREATE INDEX idx_ads_user_id ON ads(user_id);
CREATE INDEX idx_ads_task_id ON ads(task_id);
CREATE INDEX idx_ads_status ON ads(ad_status);
CREATE INDEX idx_ads_active ON ads(is_active);
CREATE INDEX idx_ads_deleted ON ads(is_deleted);
CREATE INDEX idx_ads_updated ON ads(updated_at);
CREATE INDEX idx_ads_checked ON ads(last_checked_at);
CREATE INDEX idx_ads_price ON ads(price_eur);
CREATE INDEX idx_ads_views ON ads(views);
CREATE INDEX idx_ads_start_date ON ads(start_date);
CREATE INDEX idx_ads_poster ON ads(poster_type);
CREATE INDEX idx_ads_first_seen ON ads(first_seen_at);
CREATE INDEX idx_ads_title_trgm ON ads USING gin (title gin_trgm_ops);
CREATE INDEX idx_ads_description_trgm ON ads USING gin (description gin_trgm_ops);

-- composite indexes for common filter combos
CREATE INDEX idx_ads_active_cat ON ads(category_id, is_active) WHERE is_active = true;
CREATE INDEX idx_ads_active_price ON ads(price_eur, is_active) WHERE is_active = true;
CREATE INDEX idx_ads_recheck ON ads(last_checked_at ASC NULLS FIRST) WHERE is_active = true AND is_deleted = false;

-- ==================== AD IMAGES ====================

CREATE TABLE ad_images (
    ad_id TEXT NOT NULL,
    position INTEGER NOT NULL,
    original_url TEXT DEFAULT '',
    s3_key TEXT DEFAULT '',
    preview_key TEXT DEFAULT '',
    hash TEXT DEFAULT '',
    extension TEXT DEFAULT 'jpg',
    cdn_url TEXT DEFAULT '',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ad_id, position),
    FOREIGN KEY (ad_id) REFERENCES ads(id) ON DELETE CASCADE
);

CREATE INDEX idx_images_hash ON ad_images(hash);

-- ==================== AD HISTORY ====================

CREATE TABLE ad_history (
    id BIGSERIAL PRIMARY KEY,
    ad_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    old_value TEXT DEFAULT '',
    new_value TEXT DEFAULT '',
    changed_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (ad_id) REFERENCES ads(id) ON DELETE CASCADE
);

CREATE INDEX idx_history_ad ON ad_history(ad_id);
CREATE INDEX idx_history_changed ON ad_history(changed_at);
CREATE INDEX idx_history_field ON ad_history(ad_id, field_name);
CREATE INDEX idx_history_ad_time ON ad_history(ad_id, changed_at DESC);

-- ==================== PARSE TASKS ====================

CREATE TABLE parse_tasks (
    task_id TEXT PRIMARY KEY,
    task_name TEXT DEFAULT '',
    status TEXT DEFAULT 'pending',
    category_urls JSONB DEFAULT '[]',
    filters JSONB DEFAULT '{}',
    max_pages_per_category INTEGER DEFAULT 10,
    max_ads_to_check INTEGER,
    monitor_hours INTEGER,
    user_id TEXT DEFAULT '',
    progress JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    error TEXT DEFAULT '',
    ads_count INTEGER DEFAULT 0
);

CREATE INDEX idx_tasks_user ON parse_tasks(user_id);
CREATE INDEX idx_tasks_status ON parse_tasks(status);
CREATE INDEX idx_tasks_created ON parse_tasks(created_at DESC);

-- ==================== SAVED FILTERS ====================

CREATE TABLE saved_filters (
    id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    user_id TEXT NOT NULL,
    name TEXT NOT NULL,
    filters JSONB NOT NULL DEFAULT '{}',
    category_ids TEXT[] DEFAULT '{}',
    notify_on_new BOOLEAN DEFAULT false,
    notify_on_price_drop BOOLEAN DEFAULT false,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX idx_sf_user ON saved_filters(user_id);

-- ==================== VIEWED ADS ====================

CREATE TABLE viewed_ads (
    ad_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    viewed_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ad_id, user_id),
    FOREIGN KEY (ad_id) REFERENCES ads(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- +goose Down
DROP TABLE IF EXISTS viewed_ads;
DROP TABLE IF EXISTS saved_filters;
DROP TABLE IF EXISTS parse_tasks;
DROP TABLE IF EXISTS ad_history;
DROP TABLE IF EXISTS ad_images;
DROP TABLE IF EXISTS ads;
DROP TABLE IF EXISTS categories;
DROP TABLE IF EXISTS users;
