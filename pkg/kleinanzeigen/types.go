package kleinanzeigen

import "time"

type Ad struct {
	ID             string     `json:"id" db:"id"`
	Title          string     `json:"title" db:"title"`
	Description    string     `json:"description" db:"description"`
	Price          int64      `json:"price" db:"price"`
	PriceEUR       float64    `json:"price_eur" db:"price_eur"`
	ContactName    string     `json:"contact_name" db:"contact_name"`
	CategoryID     string     `json:"category_id" db:"category_id"`
	LocationID     string     `json:"location_id" db:"location_id"`
	AdStatus       string     `json:"ad_status" db:"ad_status"`
	ShippingOption string     `json:"shipping_option" db:"shipping_option"`
	UserID         string     `json:"user_id" db:"user_id"`
	UserSinceDate  string     `json:"user_since_date" db:"user_since_date"`
	PosterType        string     `json:"poster_type" db:"poster_type"`
	SellerAccountType string     `json:"seller_account_type" db:"seller_account_type"`
	StartDate         string     `json:"start_date" db:"start_date"`
	LastEditDate      string     `json:"last_edit_date" db:"last_edit_date"`
	URL               string     `json:"url" db:"url"`
	Views             int        `json:"views" db:"views"`
	Favorites         int        `json:"favorites" db:"favorites"`
	AdType            string     `json:"ad_type" db:"ad_type"`
	PriceType         string     `json:"price_type" db:"price_type"`
	BuyNowEnabled     bool       `json:"buy_now_enabled" db:"buy_now_enabled"`
	BuyNowSelected    bool       `json:"buy_now_selected" db:"buy_now_selected"`
	BuyNowPrice       float64    `json:"buy_now_price" db:"buy_now_price"`
	UserRating        float64    `json:"user_rating" db:"user_rating"`
	IsActive       bool       `json:"is_active" db:"is_active"`
	IsDeleted      bool       `json:"is_deleted" db:"is_deleted"`
	DeletedAt      *time.Time `json:"deleted_at,omitempty" db:"deleted_at"`
	TaskID         string     `json:"task_id,omitempty" db:"task_id"`
	FirstSeenAt    time.Time  `json:"first_seen_at" db:"first_seen_at"`
	CreatedAt      time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at" db:"updated_at"`
	LastCheckedAt  time.Time  `json:"last_checked_at" db:"last_checked_at"`

	Images []AdImage `json:"images,omitempty" db:"-"`
}

type AdImage struct {
	AdID        string `json:"ad_id" db:"ad_id"`
	Position    int    `json:"position" db:"position"`
	OriginalURL string `json:"original_url" db:"original_url"`
	S3Key       string `json:"s3_key" db:"s3_key"`
	PreviewKey  string `json:"preview_key" db:"preview_key"`
	Hash        string `json:"hash" db:"hash"`
	Extension   string `json:"extension" db:"extension"`
	CDNUrl      string `json:"cdn_url" db:"cdn_url"`
}

type AdHistory struct {
	ID        int64     `json:"id" db:"id"`
	AdID      string    `json:"ad_id" db:"ad_id"`
	Field     string    `json:"field" db:"field_name"`
	OldValue  string    `json:"old_value" db:"old_value"`
	NewValue  string    `json:"new_value" db:"new_value"`
	ChangedAt time.Time `json:"changed_at" db:"changed_at"`
}

type CategoryNode struct {
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	ParentID    string         `json:"parent_id,omitempty"`
	HasChildren bool           `json:"has_children"`
	Level       int            `json:"level"`
	AdCount     int            `json:"ad_count,omitempty"`
	Children    []CategoryNode `json:"children,omitempty"`
}

type SearchParams struct {
	CategoryID string `json:"categoryId"`
	AdStatus   string `json:"adStatus"`
	Page       int    `json:"page"`
	Size       int    `json:"size"`
	MinPrice   *int   `json:"minPrice,omitempty"`
	MaxPrice   *int   `json:"maxPrice,omitempty"`
	PriceType  string `json:"priceType"`
	ModAfter   string `json:"modAfter,omitempty"`
}

type SearchResult struct {
	Ads    []RawAd                `json:"ads"`
	Paging PagingInfo             `json:"paging"`
	Raw    map[string]interface{} `json:"-"`
}

type RawAd struct {
	ID    string                 `json:"id"`
	Title map[string]interface{} `json:"title"`
	Raw   map[string]interface{} `json:"-"`
}

type PagingInfo struct {
	NumFound string `json:"numFound"`
	PageNum  int    `json:"pageNum"`
	Size     int    `json:"size"`
	Next     string `json:"next,omitempty"`
}

type ParseFilters struct {
	DateToday              bool     `json:"date_today,omitempty"`
	Last24Hours            bool     `json:"last_24_hours,omitempty"`
	DateFrom               string   `json:"date_from,omitempty"`
	DateTo                 string   `json:"date_to,omitempty"`
	PriceMin               *float64 `json:"price_min,omitempty"`
	PriceMax               *float64 `json:"price_max,omitempty"`
	ViewsMin               *int     `json:"views_min,omitempty"`
	ViewsMax               *int     `json:"views_max,omitempty"`
	SellerAdsMin           *int     `json:"seller_ads_min,omitempty"`
	SellerAdsMax           *int     `json:"seller_ads_max,omitempty"`
	SellerRegYearMin       *int     `json:"seller_registration_year_min,omitempty"`
	SellerRegYearMax       *int     `json:"seller_registration_year_max,omitempty"`
	SellerAccountStatus    string   `json:"seller_account_status,omitempty"`
	UnviewedOnly           bool     `json:"unviewed_only,omitempty"`
	DescriptionSkipPhrases []string `json:"description_skip_phrases,omitempty"`
	SicherBezahlt          bool     `json:"sicher_bezahlt,omitempty"`
	HasImages              bool     `json:"has_images,omitempty"`
	IncludeDeleted         bool     `json:"include_deleted,omitempty"`
}

type TaskStatus string

const (
	TaskPending   TaskStatus = "pending"
	TaskRunning   TaskStatus = "running"
	TaskPaused    TaskStatus = "paused"
	TaskCompleted TaskStatus = "completed"
	TaskStopped   TaskStatus = "stopped"
	TaskError     TaskStatus = "error"
)

type ParseTask struct {
	ID                  string       `json:"task_id" db:"task_id"`
	Name                string       `json:"name" db:"task_name"`
	Status              TaskStatus   `json:"status" db:"status"`
	CategoryURLs        []string     `json:"category_urls"`
	Filters             ParseFilters `json:"filters"`
	MaxPagesPerCategory int          `json:"max_pages_per_category" db:"max_pages_per_category"`
	MaxAdsToCheck       *int         `json:"max_ads_to_check,omitempty" db:"max_ads_to_check"`
	MonitorHours        *int         `json:"monitor_hours,omitempty" db:"monitor_hours"`
	UserID              string       `json:"user_id,omitempty" db:"user_id"`
	Progress            TaskProgress `json:"progress"`
	CreatedAt           time.Time    `json:"created_at" db:"created_at"`
	UpdatedAt           time.Time    `json:"updated_at" db:"updated_at"`
	CompletedAt         *time.Time   `json:"completed_at,omitempty" db:"completed_at"`
	Error               string       `json:"error,omitempty" db:"error"`
	AdsCount            int          `json:"ads_count" db:"ads_count"`
}

type TaskProgress struct {
	CategoriesProcessed int    `json:"categories_processed"`
	TotalCategories     int    `json:"total_categories"`
	AdsFound            int    `json:"ads_found"`
	AdsFiltered         int    `json:"ads_filtered"`
	AdsChecked          int    `json:"ads_checked"`
	CurrentCategory     string `json:"current_category,omitempty"`
	CurrentPage         int    `json:"current_page"`
	MonitorCycles       int    `json:"monitor_cycles"`
	Errors              int    `json:"errors"`
}

type User struct {
	ID           string    `json:"id" db:"id"`
	Email        string    `json:"email" db:"email"`
	PasswordHash string    `json:"-" db:"password_hash"`
	Name         string    `json:"name" db:"name"`
	Role         string    `json:"role" db:"role"`
	IsAdmin      bool      `json:"is_admin" db:"is_admin"`
	CreatedAt    time.Time `json:"created_at" db:"created_at"`
	LastLoginAt  time.Time `json:"last_login_at,omitempty" db:"last_login_at"`
}

type SavedFilter struct {
	ID               string       `json:"id" db:"id"`
	UserID           string       `json:"user_id" db:"user_id"`
	Name             string       `json:"name" db:"name"`
	Filters          ParseFilters `json:"filters"`
	CategoryIDs      []string     `json:"category_ids"`
	NotifyOnNew      bool         `json:"notify_on_new" db:"notify_on_new"`
	NotifyOnPriceDrop bool        `json:"notify_on_price_drop" db:"notify_on_price_drop"`
	CreatedAt        time.Time    `json:"created_at" db:"created_at"`
	UpdatedAt        time.Time    `json:"updated_at" db:"updated_at"`
}

// AdMetrics — pre-computed metrics for instant filtering.
// Lives in ad_metrics table, updated on every snapshot.
type AdMetrics struct {
	AdID             string    `json:"ad_id" db:"ad_id"`
	ViewsCurrent     int       `json:"views_current" db:"views_current"`
	FavoritesCurrent int       `json:"favorites_current" db:"favorites_current"`
	PriceCurrent     float64   `json:"price_current" db:"price_current"`
	Views1hAgo       int       `json:"views_1h_ago" db:"views_1h_ago"`
	Views24hAgo      int       `json:"views_24h_ago" db:"views_24h_ago"`
	Views7dAgo       int       `json:"views_7d_ago" db:"views_7d_ago"`
	ViewsDelta1h     int       `json:"views_delta_1h" db:"views_delta_1h"`
	ViewsDelta24h    int       `json:"views_delta_24h" db:"views_delta_24h"`
	ViewsDelta7d     int       `json:"views_delta_7d" db:"views_delta_7d"`
	ViewsPerHour     float64   `json:"views_per_hour" db:"views_per_hour"`
	Favorites1hAgo   int       `json:"favorites_1h_ago" db:"favorites_1h_ago"`
	Favorites24hAgo  int       `json:"favorites_24h_ago" db:"favorites_24h_ago"`
	Favorites7dAgo   int       `json:"favorites_7d_ago" db:"favorites_7d_ago"`
	FavoritesDelta1h  int      `json:"favorites_delta_1h" db:"favorites_delta_1h"`
	FavoritesDelta24h int      `json:"favorites_delta_24h" db:"favorites_delta_24h"`
	FavoritesDelta7d  int      `json:"favorites_delta_7d" db:"favorites_delta_7d"`
	FavoritesPerHour  float64  `json:"favorites_per_hour" db:"favorites_per_hour"`
	PricePrevious    float64   `json:"price_previous" db:"price_previous"`
	PriceMinSeen     float64   `json:"price_min_seen" db:"price_min_seen"`
	PriceMaxSeen     float64   `json:"price_max_seen" db:"price_max_seen"`
	PriceDropped     bool      `json:"price_dropped" db:"price_dropped"`
	PriceChangePct   float64   `json:"price_change_pct" db:"price_change_pct"`
	PriceChangeCount int       `json:"price_change_count,omitempty" db:"price_change_count"`
	SnapshotCount    int       `json:"snapshot_count" db:"snapshot_count"`
	FirstSeenAt      time.Time `json:"first_seen_at" db:"first_seen_at"`
	LastSnapshotAt   time.Time `json:"last_snapshot_at" db:"last_snapshot_at"`
}

type AdSearchRequest struct {
	Query       string   `json:"q,omitempty"`
	Exclude     []string `json:"exclude,omitempty"`
	CategoryIDs []string `json:"category_ids,omitempty"`
	LocationIDs []string `json:"location_ids,omitempty"`
	SellerID    string   `json:"seller_id,omitempty"`
	PriceMin    *float64 `json:"price_min,omitempty"`
	PriceMax    *float64 `json:"price_max,omitempty"`
	ViewsMin    *int     `json:"views_min,omitempty"`
	ViewsMax    *int     `json:"views_max,omitempty"`
	PosterType  string   `json:"poster_type,omitempty"`
	IsActive    *bool    `json:"is_active,omitempty"`
	IsDeleted   *bool    `json:"is_deleted,omitempty"`
	DateFrom    string   `json:"date_from,omitempty"`
	DateTo      string   `json:"date_to,omitempty"`
	HasImages   *bool    `json:"has_images,omitempty"`
	TaskID      string   `json:"task_id,omitempty"`

	FavoritesMin *int `json:"favorites_min,omitempty"`
	FavoritesMax *int `json:"favorites_max,omitempty"`

	ViewsDelta1hMin  *int     `json:"views_delta_1h_min,omitempty"`
	ViewsDelta1hMax  *int     `json:"views_delta_1h_max,omitempty"`
	ViewsDelta24hMin *int     `json:"views_delta_24h_min,omitempty"`
	ViewsDelta24hMax *int     `json:"views_delta_24h_max,omitempty"`
	ViewsPerHourMin  *float64 `json:"views_per_hour_min,omitempty"`

	FavoritesDelta1hMin  *int     `json:"favorites_delta_1h_min,omitempty"`
	FavoritesDelta24hMin *int     `json:"favorites_delta_24h_min,omitempty"`
	FavoritesPerHourMin  *float64 `json:"favorites_per_hour_min,omitempty"`

	PriceDropped     *bool    `json:"price_dropped,omitempty"`

	SortBy    string `json:"sort_by,omitempty"`
	SortOrder string `json:"sort_order,omitempty"`
	Page      int    `json:"page"`
	PerPage   int    `json:"per_page"`
}

// AdWithMetrics — what the search API returns: ad + its live metrics inline
type AdWithMetrics struct {
	Ad
	Metrics   *AdMetrics `json:"metrics,omitempty"`
	Thumbnail string     `json:"thumbnail,omitempty"`
}

type ChartPoint struct {
	Timestamp time.Time `json:"t"`
	Value     float64   `json:"v"`
}

type AdChartData struct {
	AdID         string       `json:"ad_id"`
	ViewsChart      []ChartPoint `json:"views"`
	FavoritesChart  []ChartPoint `json:"favorites"`
	PriceChart      []ChartPoint `json:"price"`
	HourlyViews  []ChartPoint `json:"hourly_views,omitempty"`
	HourlyDelta  []ChartPoint `json:"hourly_delta,omitempty"`
	HourlyPrice  []ChartPoint `json:"hourly_price,omitempty"`
	DailyViews   []ChartPoint `json:"daily_views,omitempty"`
	DailyDelta   []ChartPoint `json:"daily_delta,omitempty"`
}

type AdStatistics struct {
	AdID             string     `json:"ad_id"`
	Metrics          *AdMetrics `json:"metrics"`
	Chart            *AdChartData `json:"chart,omitempty"`
	TotalChanges     int        `json:"total_changes"`
}

type DashboardStats struct {
	TotalAds       int64          `json:"total_ads"`
	ActiveAds      int64          `json:"active_ads"`
	DeletedAds     int64          `json:"deleted_ads"`
	TotalImages    int64          `json:"total_images"`
	AdsToday       int64          `json:"ads_today"`
	AvgPrice       float64        `json:"avg_price"`
	TopCategories  []CategoryStat `json:"top_categories"`
	TrendingAds    int64          `json:"trending_ads"`
	PriceDrops     int64          `json:"price_drops_24h"`
	TotalSnapshots int64          `json:"total_snapshots"`
	ProxyCount     int            `json:"proxy_count"`
	RunningTasks   int            `json:"running_tasks"`
}

type CategoryStat struct {
	CategoryID string `json:"category_id"`
	Name       string `json:"name"`
	Count      int64  `json:"count"`
}

type Notification struct {
	ID        int64                  `json:"id" db:"id"`
	UserID    string                 `json:"user_id" db:"user_id"`
	Type      string                 `json:"type" db:"type"`
	Title     string                 `json:"title" db:"title"`
	Body      string                 `json:"body" db:"body"`
	Data      map[string]interface{} `json:"data,omitempty"`
	IsRead    bool                   `json:"is_read" db:"is_read"`
	CreatedAt time.Time              `json:"created_at" db:"created_at"`
}

type SellerProfile struct {
	UserID    string `json:"user_id"`
	Name      string `json:"name"`
	SinceDate string `json:"since_date"`
	TotalAds  int    `json:"total_ads"`
	ActiveAds int    `json:"active_ads"`
}
