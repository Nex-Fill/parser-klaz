package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"

	kl "github.com/danamakarenko/klaz-parser/pkg/kleinanzeigen"
)

type Cache struct {
	rdb      *redis.Client
	cacheTTL time.Duration
}

func NewCache(addr, password string, db int, ttl time.Duration) (*Cache, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		PoolSize:     100,
		MinIdleConns: 10,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	return &Cache{rdb: rdb, cacheTTL: ttl}, nil
}

func (c *Cache) Close() error {
	return c.rdb.Close()
}

func (c *Cache) Client() *redis.Client {
	return c.rdb
}

func (c *Cache) CacheAd(ctx context.Context, ad *kl.Ad) error {
	data, err := json.Marshal(ad)
	if err != nil {
		return err
	}
	return c.rdb.Set(ctx, adKey(ad.ID), data, c.cacheTTL).Err()
}

func (c *Cache) CacheAdsBatch(ctx context.Context, ads []*kl.Ad) error {
	pipe := c.rdb.Pipeline()
	for _, ad := range ads {
		data, err := json.Marshal(ad)
		if err != nil {
			continue
		}
		pipe.Set(ctx, adKey(ad.ID), data, c.cacheTTL)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (c *Cache) GetAd(ctx context.Context, id string) (*kl.Ad, error) {
	data, err := c.rdb.Get(ctx, adKey(id)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var ad kl.Ad
	if err := json.Unmarshal(data, &ad); err != nil {
		return nil, err
	}
	return &ad, nil
}

func (c *Cache) InvalidateAd(ctx context.Context, id string) {
	c.rdb.Del(ctx, adKey(id))
}

func (c *Cache) CacheTaskProgress(ctx context.Context, task *kl.ParseTask) error {
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return c.rdb.Set(ctx, taskKey(task.ID), data, 0).Err()
}

func (c *Cache) GetTaskProgress(ctx context.Context, taskID string) (*kl.ParseTask, error) {
	data, err := c.rdb.Get(ctx, taskKey(taskID)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var task kl.ParseTask
	if err := json.Unmarshal(data, &task); err != nil {
		return nil, err
	}
	return &task, nil
}

func (c *Cache) CacheSearchResult(ctx context.Context, key string, ads []*kl.Ad, total int) error {
	data, err := json.Marshal(map[string]interface{}{
		"ads":   ads,
		"total": total,
	})
	if err != nil {
		return err
	}
	return c.rdb.Set(ctx, "search:"+key, data, 2*time.Minute).Err()
}

func (c *Cache) GetSearchResult(ctx context.Context, key string) ([]*kl.Ad, int, bool) {
	data, err := c.rdb.Get(ctx, "search:"+key).Bytes()
	if err != nil {
		return nil, 0, false
	}

	var result struct {
		Ads   []*kl.Ad `json:"ads"`
		Total int      `json:"total"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, 0, false
	}
	return result.Ads, result.Total, true
}

func (c *Cache) PublishAdUpdate(ctx context.Context, ad *kl.Ad) {
	data, _ := json.Marshal(ad)
	if err := c.rdb.Publish(ctx, "ad:updates", data).Err(); err != nil {
		log.Warn().Err(err).Str("ad_id", ad.ID).Msg("failed to publish ad update")
	}
}

func (c *Cache) PublishTaskUpdate(ctx context.Context, task *kl.ParseTask) {
	data, _ := json.Marshal(task)
	if err := c.rdb.Publish(ctx, "task:updates", data).Err(); err != nil {
		log.Warn().Err(err).Str("task_id", task.ID).Msg("failed to publish task update")
	}
}

func (c *Cache) IncrementCounter(ctx context.Context, key string) int64 {
	val, _ := c.rdb.Incr(ctx, "counter:"+key).Result()
	return val
}

func (c *Cache) GetStats(ctx context.Context) map[string]int64 {
	stats := make(map[string]int64)
	keys := []string{"ads_parsed", "ads_checked", "images_uploaded", "proxy_errors"}
	for _, k := range keys {
		val, _ := c.rdb.Get(ctx, "counter:"+k).Int64()
		stats[k] = val
	}
	return stats
}

func adKey(id string) string   { return "ad:" + id }
func taskKey(id string) string { return "task:" + id }
