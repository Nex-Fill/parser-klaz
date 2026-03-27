package config

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

type LiveSettings struct {
	RecheckFreshInterval   time.Duration `json:"recheck_fresh_interval"`
	RecheckPopularInterval time.Duration `json:"recheck_popular_interval"`
	RecheckNormalInterval  time.Duration `json:"recheck_normal_interval"`
	RecheckArchiveInterval time.Duration `json:"recheck_archive_interval"`

	ParseWorkers int `json:"parse_workers"`
	ImageWorkers int `json:"image_workers"`

	ProxySourceURL      string        `json:"proxy_source_url"`
	ProxyUpdateInterval time.Duration `json:"proxy_update_interval"`

	AutoRecheckEnabled bool     `json:"auto_recheck_enabled"`
	ImageUploadEnabled bool     `json:"image_upload_enabled"`
	CategoryBlacklist  []string `json:"category_blacklist"`

	FreeTierTaskLimit    int `json:"free_tier_task_limit"`
	PremiumTierTaskLimit int `json:"premium_tier_task_limit"`

	UpdatedAt time.Time `json:"updated_at"`
}

func DefaultLiveSettings(cfg *Config) *LiveSettings {
	return &LiveSettings{
		RecheckFreshInterval:   30 * time.Minute,
		RecheckPopularInterval: 1 * time.Hour,
		RecheckNormalInterval:  4 * time.Hour,
		RecheckArchiveInterval: 24 * time.Hour,
		ParseWorkers:           cfg.Workers.ParseWorkers,
		ImageWorkers:           cfg.Workers.ImageWorkers,
		ProxySourceURL:         cfg.Proxy.FileURL,
		ProxyUpdateInterval:    cfg.Proxy.UpdateInterval,
		AutoRecheckEnabled:     true,
		ImageUploadEnabled:     true,
		CategoryBlacklist:      []string{},
		FreeTierTaskLimit:      3,
		PremiumTierTaskLimit:   20,
		UpdatedAt:              time.Now(),
	}
}

type LiveConfig struct {
	current  atomic.Pointer[LiveSettings]
	rdb      *redis.Client
	pgPool   *pgxpool.Pool
	defaults *LiveSettings
}

const (
	redisConfigKey     = "config:live"
	redisConfigChannel = "config:changed"
)

func NewLiveConfig(rdb *redis.Client, pgPool *pgxpool.Pool, defaults *LiveSettings) *LiveConfig {
	lc := &LiveConfig{rdb: rdb, pgPool: pgPool, defaults: defaults}
	lc.current.Store(defaults)
	return lc
}

func (lc *LiveConfig) Get() *LiveSettings {
	return lc.current.Load()
}

func (lc *LiveConfig) Load(ctx context.Context) {
	if s, err := lc.loadFromRedis(ctx); err == nil && s != nil {
		lc.current.Store(s)
		return
	}
	if s, err := lc.loadFromPostgres(ctx); err == nil && s != nil {
		lc.current.Store(s)
		lc.writeToRedis(ctx, s)
		return
	}
	log.Warn().Msg("using default live settings")
}

func (lc *LiveConfig) Update(ctx context.Context, patch map[string]interface{}, updatedBy string) error {
	current := lc.Get()
	merged := *current

	for k, v := range patch {
		applyPatch(&merged, k, v)
	}
	merged.UpdatedAt = time.Now()

	lc.current.Store(&merged)

	if err := lc.writeToRedis(ctx, &merged); err != nil {
		log.Warn().Err(err).Msg("failed to write config to redis")
	}
	if err := lc.persistToPostgres(ctx, patch, updatedBy); err != nil {
		log.Warn().Err(err).Msg("failed to persist config to postgres")
	}

	lc.rdb.Publish(ctx, redisConfigChannel, "updated")
	return nil
}

func (lc *LiveConfig) GetAll(ctx context.Context) map[string]interface{} {
	s := lc.Get()
	data, err := json.Marshal(s)
	if err != nil {
		return map[string]interface{}{}
	}
	result := map[string]interface{}{}
	if err := json.Unmarshal(data, &result); err != nil {
		return map[string]interface{}{}
	}
	return result
}

func (lc *LiveConfig) StartWatcher(ctx context.Context) {
	go func() {
		pubsub := lc.rdb.Subscribe(ctx, redisConfigChannel)
		defer pubsub.Close()
		ch := pubsub.Channel()

		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-ch:
				if !ok {
					return
				}
				if s, err := lc.loadFromRedis(ctx); err == nil && s != nil {
					lc.current.Store(s)
					log.Info().Msg("live config reloaded from redis pub/sub")
				}
			}
		}
	}()
	log.Info().Msg("live config watcher started")
}

func (lc *LiveConfig) loadFromRedis(ctx context.Context) (*LiveSettings, error) {
	data, err := lc.rdb.Get(ctx, redisConfigKey).Bytes()
	if err != nil {
		return nil, err
	}
	var s LiveSettings
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

func (lc *LiveConfig) writeToRedis(ctx context.Context, s *LiveSettings) error {
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return lc.rdb.Set(ctx, redisConfigKey, data, 0).Err()
}

func (lc *LiveConfig) loadFromPostgres(ctx context.Context) (*LiveSettings, error) {
	rows, err := lc.pgPool.Query(ctx, "SELECT key, value FROM system_settings")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	s := *lc.defaults
	for rows.Next() {
		var key string
		var value json.RawMessage
		if rows.Scan(&key, &value) != nil {
			continue
		}
		applyJSONValue(&s, key, value)
	}
	s.UpdatedAt = time.Now()
	return &s, nil
}

func (lc *LiveConfig) persistToPostgres(ctx context.Context, patch map[string]interface{}, updatedBy string) error {
	var lastErr error
	for k, v := range patch {
		val, err := json.Marshal(v)
		if err != nil {
			lastErr = err
			continue
		}
		if _, err := lc.pgPool.Exec(ctx, `
			INSERT INTO system_settings (key, value, updated_at, updated_by)
			VALUES ($1, $2, NOW(), $3)
			ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = NOW(), updated_by = $3`,
			k, val, updatedBy); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func applyPatch(s *LiveSettings, key string, val interface{}) {
	switch key {
	case "recheck_fresh_interval":
		s.RecheckFreshInterval = parseDuration(val)
	case "recheck_popular_interval":
		s.RecheckPopularInterval = parseDuration(val)
	case "recheck_normal_interval":
		s.RecheckNormalInterval = parseDuration(val)
	case "recheck_archive_interval":
		s.RecheckArchiveInterval = parseDuration(val)
	case "parse_workers":
		s.ParseWorkers = parseInt(val)
	case "image_workers":
		s.ImageWorkers = parseInt(val)
	case "proxy_source_url":
		if v, ok := val.(string); ok {
			s.ProxySourceURL = v
		}
	case "proxy_update_interval":
		s.ProxyUpdateInterval = parseDuration(val)
	case "auto_recheck_enabled":
		if v, ok := val.(bool); ok {
			s.AutoRecheckEnabled = v
		}
	case "image_upload_enabled":
		if v, ok := val.(bool); ok {
			s.ImageUploadEnabled = v
		}
	case "category_blacklist":
		if v, ok := val.([]interface{}); ok {
			s.CategoryBlacklist = make([]string, 0, len(v))
			for _, item := range v {
				if str, ok := item.(string); ok {
					s.CategoryBlacklist = append(s.CategoryBlacklist, str)
				}
			}
		}
	case "free_tier_task_limit":
		s.FreeTierTaskLimit = parseInt(val)
	case "premium_tier_task_limit":
		s.PremiumTierTaskLimit = parseInt(val)
	}
}

func applyJSONValue(s *LiveSettings, key string, raw json.RawMessage) {
	var val interface{}
	json.Unmarshal(raw, &val)
	applyPatch(s, key, val)
}

func parseDuration(val interface{}) time.Duration {
	switch v := val.(type) {
	case string:
		d, _ := time.ParseDuration(v)
		return d
	case float64:
		return time.Duration(v) * time.Second
	}
	return 0
}

func parseInt(val interface{}) int {
	switch v := val.(type) {
	case float64:
		return int(v)
	case int:
		return v
	}
	return 0
}
