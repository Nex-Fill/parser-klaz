package parser

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/danamakarenko/klaz-parser/internal/config"
	"github.com/danamakarenko/klaz-parser/internal/proxy"
	"github.com/danamakarenko/klaz-parser/internal/storage"
	kl "github.com/danamakarenko/klaz-parser/pkg/kleinanzeigen"
)

type Scraper struct {
	cfg        config.ParserConfig
	liveConfig *config.LiveConfig
	proxyPool  *proxy.Pool
	db         *storage.Postgres
	cache      *storage.Cache
	media      MediaPipeline
	pool       *ants.Pool
	filter     *Filter
	snapBuf    *storage.SnapshotBuffer
}

type MediaPipeline interface {
	ProcessImages(ctx context.Context, adID string, urls []string) ([]kl.AdImage, error)
}

func NewScraper(cfg config.ParserConfig, lc *config.LiveConfig, proxyPool *proxy.Pool, db *storage.Postgres, cache *storage.Cache, media MediaPipeline, workerCount int) (*Scraper, error) {
	pool, err := ants.NewPool(workerCount, ants.WithPreAlloc(true), ants.WithPanicHandler(func(i interface{}) {
		log.Error().Interface("panic", i).Msg("worker panic recovered")
	}))
	if err != nil {
		return nil, err
	}
	snapBuf := storage.NewSnapshotBuffer(db, 5000, 3*time.Second)

	return &Scraper{
		cfg: cfg, liveConfig: lc, proxyPool: proxyPool, db: db, cache: cache,
		media: media, pool: pool, filter: NewFilter(), snapBuf: snapBuf,
	}, nil
}

func (s *Scraper) Close() {
	s.snapBuf.Stop()
	s.pool.Release()
}

func (s *Scraper) RunTask(ctx context.Context, task *kl.ParseTask) error {
	log.Info().Str("task_id", task.ID).Int("categories", len(task.CategoryURLs)).Msg("starting parse task")

	task.Status = kl.TaskRunning
	task.UpdatedAt = time.Now()
	s.updateTask(ctx, task)

	var totalFound, totalChecked atomic.Int64

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(len(task.CategoryURLs))

	for i, catURL := range task.CategoryURLs {
		catURL := catURL
		catIdx := i
		g.Go(func() error {
			catID := kl.ExtractCategoryID(catURL)
			if catID == "" {
				log.Warn().Str("url", catURL).Msg("failed to extract category ID")
				return nil
			}
			found, checked, err := s.parseCategory(gCtx, task, catID, catIdx)
			if err != nil {
				log.Error().Err(err).Str("category", catID).Msg("category parse error")
				task.Progress.Errors++
			}
			totalFound.Add(int64(found))
			totalChecked.Add(int64(checked))
			task.Progress.CategoriesProcessed = catIdx + 1
			task.Progress.AdsFound = int(totalFound.Load())
			task.Progress.AdsChecked = int(totalChecked.Load())
			task.UpdatedAt = time.Now()
			s.updateTask(gCtx, task)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		task.Status = kl.TaskError
		task.Error = err.Error()
	} else {
		task.Status = kl.TaskCompleted
		now := time.Now()
		task.CompletedAt = &now
	}
	task.AdsCount = int(totalFound.Load())
	task.UpdatedAt = time.Now()
	s.updateTask(ctx, task)
	return nil
}

func (s *Scraper) parseCategory(ctx context.Context, task *kl.ParseTask, catID string, catIdx int) (found, checked int, err error) {
	maxPages := s.cfg.MaxPagesPerCat
	if task.MaxPagesPerCategory > 0 {
		maxPages = task.MaxPagesPerCategory
	}

	for page := 0; page < maxPages; page++ {
		select {
		case <-ctx.Done():
			return found, checked, ctx.Err()
		default:
		}
		if task.Status == kl.TaskStopped {
			return found, checked, nil
		}
		for task.Status == kl.TaskPaused {
			time.Sleep(time.Second)
			if task.Status == kl.TaskStopped {
				return found, checked, nil
			}
		}

		task.Progress.CurrentCategory = catID
		task.Progress.CurrentPage = page

		params := kl.SearchParams{
			CategoryID: catID, AdStatus: "ACTIVE",
			Page: page, Size: s.cfg.PageSize,
		}
		if task.Filters.PriceMin != nil {
			v := int(*task.Filters.PriceMin)
			params.MinPrice = &v
		}
		if task.Filters.PriceMax != nil {
			v := int(*task.Filters.PriceMax)
			params.MaxPrice = &v
		}

		searchResult, err := s.searchAds(ctx, params)
		if err != nil {
			task.Progress.Errors++
			break
		}
		if len(searchResult.Ads) == 0 {
			break
		}

		pf, pc := s.processBatch(ctx, task, searchResult.Ads, catID)
		found += pf
		checked += pc

		if task.MaxAdsToCheck != nil && checked >= *task.MaxAdsToCheck {
			break
		}
		if len(searchResult.Ads) < params.Size {
			break
		}
		total, _ := strconv.Atoi(searchResult.Paging.NumFound)
		if total > 0 && (page+1)*params.Size >= total {
			break
		}
	}
	return
}

func (s *Scraper) processBatch(ctx context.Context, task *kl.ParseTask, rawAds []kl.RawAd, catID string) (found, checked int) {
	batchSize := s.cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 50
	}
	skipViews := strings.HasPrefix(task.ID, "auto_")
	skipImages := skipViews && !s.liveConfig.Get().ImageUploadEnabled

	for i := 0; i < len(rawAds); i += batchSize {
		end := i + batchSize
		if end > len(rawAds) {
			end = len(rawAds)
		}
		batch := rawAds[i:end]

		var mu sync.Mutex
		var ads []*kl.Ad
		var allImages []kl.AdImage
		var wg sync.WaitGroup

		for _, raw := range batch {
			raw := raw
			wg.Add(1)
			s.pool.Submit(func() {
				defer wg.Done()
				var ad *kl.Ad
				var photos []string
				var err error
				if skipViews {
					ad, photos, err = s.fetchAndParseAdFast(ctx, raw.ID)
				} else {
					ad, photos, err = s.fetchAndParseAd(ctx, raw.ID)
				}
				if err != nil {
					return
				}
				ad.TaskID = task.ID
				ad.CategoryID = catID
				if !s.filter.Apply(ad, &task.Filters) {
					return
				}
				var images []kl.AdImage
				if !skipImages && s.media != nil && len(photos) > 0 {
					images, _ = s.media.ProcessImages(ctx, ad.ID, photos)
				}
				s.snapBuf.Record(ad.ID, ad.Views, ad.PriceEUR)

				mu.Lock()
				ads = append(ads, ad)
				allImages = append(allImages, images...)
				mu.Unlock()
			})
		}
		wg.Wait()
		checked += len(batch)
	}
	return
}

func (s *Scraper) fetchAndParseAd(ctx context.Context, adID string) (*kl.Ad, []string, error) {
	type adResult struct {
		body []byte
		err  error
	}
	type viewsResult struct {
		views int
		err   error
	}

	adCh := make(chan adResult, 1)
	viewsCh := make(chan viewsResult, 1)

	go func() {
		body, err := s.doRequest(ctx, kl.BuildAdURL(adID))
		adCh <- adResult{body, err}
	}()
	go func() {
		v, err := s.fetchViews(ctx, adID)
		viewsCh <- viewsResult{v, err}
	}()

	ar := <-adCh
	if ar.err != nil {
		<-viewsCh
		return nil, nil, fmt.Errorf("fetch ad %s: %w", adID, ar.err)
	}

	ad, photos, err := kl.ParseAdResponse(ar.body)
	if err != nil {
		<-viewsCh
		return nil, nil, fmt.Errorf("parse ad %s: %w", adID, err)
	}

	vr := <-viewsCh
	if vr.err == nil {
		ad.Views = vr.views
	}

	return ad, photos, nil
}

func (s *Scraper) fetchAndParseAdFast(ctx context.Context, adID string) (*kl.Ad, []string, error) {
	body, err := s.doRequest(ctx, kl.BuildAdURL(adID))
	if err != nil {
		return nil, nil, fmt.Errorf("fetch ad %s: %w", adID, err)
	}
	ad, photos, err := kl.ParseAdResponse(body)
	if err != nil {
		return nil, nil, fmt.Errorf("parse ad %s: %w", adID, err)
	}
	return ad, photos, nil
}

func (s *Scraper) fetchViews(ctx context.Context, adID string) (int, error) {
	viewsURL := kl.BuildViewsURL(adID)
	proxyURL := s.proxyPool.GetWeighted()
	client := s.buildClient(proxyURL)

	req, err := http.NewRequestWithContext(ctx, "GET", viewsURL, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "de-DE,de;q=0.9")
	req.Header.Set("Referer", fmt.Sprintf("https://www.kleinanzeigen.de/s-anzeige/%s", adID))

	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("views http %d", resp.StatusCode)
	}

	var reader io.Reader = resp.Body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gr, err := gzip.NewReader(resp.Body)
		if err == nil {
			reader = gr
			defer gr.Close()
		}
	}

	body, err := io.ReadAll(reader)
	if err != nil {
		return 0, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, err
	}
	if v, ok := result["numVisits"].(float64); ok {
		return int(v), nil
	}
	if v, ok := result["counter"].(float64); ok {
		return int(v), nil
	}
	return 0, nil
}

func (s *Scraper) searchAds(ctx context.Context, params kl.SearchParams) (*kl.SearchResult, error) {
	body, err := s.doRequest(ctx, kl.BuildSearchURL(params))
	if err != nil {
		return nil, err
	}
	return kl.ParseSearchResponse(body)
}

func (s *Scraper) doRequest(ctx context.Context, targetURL string) ([]byte, error) {
	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		proxyURL := s.proxyPool.GetWeighted()
		client := s.buildClient(proxyURL)
		headers := kl.GenerateAPIHeaders(s.cfg.KlazVersion, s.cfg.BasicAuth)

		req, err := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
		if err != nil {
			return nil, err
		}
		for k, v := range headers {
			req.Header[k] = v
		}

		start := time.Now()
		resp, err := client.Do(req)
		latency := time.Since(start)

		if err != nil {
			if proxyURL != "" {
				s.proxyPool.ReportFailure(proxyURL)
			}
			continue
		}
		var reader io.Reader = resp.Body
		if resp.Header.Get("Content-Encoding") == "gzip" {
			gr, err := gzip.NewReader(resp.Body)
			if err == nil {
				reader = gr
				defer gr.Close()
			}
		}
		body, _ := io.ReadAll(reader)
		resp.Body.Close()

		if resp.StatusCode == 200 {
			if proxyURL != "" {
				s.proxyPool.ReportSuccess(proxyURL, latency)
			}
			return body, nil
		}
		if proxyURL != "" {
			s.proxyPool.ReportFailure(proxyURL)
		}
		if resp.StatusCode == 404 {
			return nil, fmt.Errorf("404_NOT_FOUND")
		}
		if resp.StatusCode == 429 {
			time.Sleep(time.Duration(attempt+1) * 2 * time.Second)
		}
	}
	return nil, fmt.Errorf("all retries exhausted for %s", targetURL)
}

func (s *Scraper) buildClient(proxyURL string) *http.Client {
	transport := &http.Transport{
		MaxIdleConns: 100, MaxIdleConnsPerHost: 10, IdleConnTimeout: 30 * time.Second,
	}
	if proxyURL != "" {
		if u, err := url.Parse(proxyURL); err == nil {
			transport.Proxy = http.ProxyURL(u)
		}
	}
	return &http.Client{Transport: transport, Timeout: s.cfg.RequestTimeout}
}

func (s *Scraper) updateTask(ctx context.Context, task *kl.ParseTask) {
	s.cache.CacheTaskProgress(ctx, task)
	s.cache.PublishTaskUpdate(ctx, task)
	if err := s.db.SaveTask(ctx, task); err != nil {
		log.Warn().Err(err).Str("task_id", task.ID).Msg("task save failed")
	}
}

// ==================== PRIORITY RECHECK ====================
//
// Priority 1: fresh ads (<24h old)              — every 30 min
// Priority 2: popular ads (views > 50)          — every 1 hour
// Priority 3: normal active ads                 — every 4 hours
// Priority 4: low-traffic old ads               — every 24 hours
// Blacklist:  is_deleted=true                   — never

type RecheckTier struct {
	Name     string
	Interval time.Duration
	Query    string
}

func (s *Scraper) buildRecheckTiers() []RecheckTier {
	lc := s.liveConfig.Get()
	blacklistClause := ""
	if len(lc.CategoryBlacklist) > 0 {
		blacklistClause = " AND category_id NOT IN ('" + strings.Join(lc.CategoryBlacklist, "','") + "')"
	}

	return []RecheckTier{
		{
			Name:     "fresh",
			Interval: lc.RecheckFreshInterval,
			Query: `SELECT id FROM ads
				WHERE is_active = true AND is_deleted = false
				AND first_seen_at >= NOW() - INTERVAL '24 hours'
				AND (last_checked_at < $1 OR last_checked_at IS NULL)` + blacklistClause + `
				ORDER BY last_checked_at ASC NULLS FIRST`,
		},
		{
			Name:     "popular",
			Interval: lc.RecheckPopularInterval,
			Query: `SELECT id FROM ads
				WHERE is_active = true AND is_deleted = false
				AND views > 50
				AND first_seen_at < NOW() - INTERVAL '24 hours'
				AND (last_checked_at < $1 OR last_checked_at IS NULL)` + blacklistClause + `
				ORDER BY views DESC, last_checked_at ASC NULLS FIRST`,
		},
		{
			Name:     "normal",
			Interval: lc.RecheckNormalInterval,
			Query: `SELECT id FROM ads
				WHERE is_active = true AND is_deleted = false
				AND views <= 50
				AND first_seen_at < NOW() - INTERVAL '24 hours'
				AND first_seen_at >= NOW() - INTERVAL '30 days'
				AND (last_checked_at < $1 OR last_checked_at IS NULL)` + blacklistClause + `
				ORDER BY last_checked_at ASC NULLS FIRST`,
		},
		{
			Name:     "archive",
			Interval: lc.RecheckArchiveInterval,
			Query: `SELECT id FROM ads
				WHERE is_active = true AND is_deleted = false
				AND first_seen_at > '2000-01-01'
				AND first_seen_at < NOW() - INTERVAL '30 days'
				AND (last_checked_at < $1 OR last_checked_at IS NULL)` + blacklistClause + `
				ORDER BY last_checked_at ASC NULLS FIRST`,
		},
		{
			Name:     "stale",
			Interval: 1 * time.Hour,
			Query: `SELECT id FROM ads
				WHERE is_active = true AND is_deleted = false
				AND (last_checked_at < $1 OR last_checked_at IS NULL)` + blacklistClause + `
				ORDER BY last_checked_at ASC NULLS FIRST`,
		},
	}
}

func (s *Scraper) RecheckByPriority(ctx context.Context) error {
	if !s.liveConfig.Get().AutoRecheckEnabled {
		log.Info().Msg("auto recheck disabled, skipping")
		return nil
	}

	tiers := s.buildRecheckTiers()
	for _, tier := range tiers {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		cutoff := time.Now().Add(-tier.Interval)
		ids, err := s.db.QueryAdIDs(ctx, tier.Query, cutoff)
		if err != nil {
			log.Error().Err(err).Str("tier", tier.Name).Msg("recheck query failed")
			continue
		}
		if len(ids) == 0 {
			log.Debug().Str("tier", tier.Name).Msg("nothing to recheck")
			continue
		}

		log.Info().Str("tier", tier.Name).Int("count", len(ids)).Msg("recheck tier started")
		start := time.Now()

		s.recheckBatch(ctx, ids)

		log.Info().
			Str("tier", tier.Name).
			Int("count", len(ids)).
			Dur("took", time.Since(start)).
			Msg("recheck tier done")
	}
	return nil
}

func (s *Scraper) recheckBatch(ctx context.Context, ids []string) {
	batchSize := 500
	for i := 0; i < len(ids); i += batchSize {
		select {
		case <-ctx.Done():
			return
		default:
		}

		end := i + batchSize
		if end > len(ids) {
			end = len(ids)
		}
		chunk := ids[i:end]

		var wg sync.WaitGroup
		for _, id := range chunk {
			id := id
			wg.Add(1)
			s.pool.Submit(func() {
				defer wg.Done()
				s.recheckOneAd(ctx, id)
			})
		}
		wg.Wait()
	}
}

// InstantRecheck — triggered by user viewing an ad. Returns fresh data.
func (s *Scraper) InstantRecheck(ctx context.Context, adID string) (*kl.Ad, error) {
	existing, _ := s.db.GetAd(ctx, adID)

	ad, photos, err := s.fetchAndParseAd(ctx, adID)
	if err != nil {
		if strings.Contains(err.Error(), "404_NOT_FOUND") {
			s.db.MarkAdDeleted(ctx, adID)
			if existing != nil {
				s.db.RecordHistory(ctx, adID, "ad_status", existing.AdStatus, "DELETED")
			}
			s.cache.InvalidateAd(ctx, adID)
			if existing != nil {
				existing.IsDeleted = true
				existing.IsActive = false
				existing.AdStatus = "DELETED"
				return existing, nil
			}
			return nil, fmt.Errorf("ad deleted")
		}
		return nil, err
	}

	if existing != nil {
		s.trackChanges(ctx, existing, ad)
		ad.FirstSeenAt = existing.FirstSeenAt
		ad.TaskID = existing.TaskID
	}

	s.db.UpsertAd(ctx, ad)
	s.snapBuf.Record(adID, ad.Views, ad.PriceEUR)
	s.cache.CacheAd(ctx, ad)
	s.cache.PublishAdUpdate(ctx, ad)

	if s.media != nil && len(photos) > 0 && s.liveConfig.Get().ImageUploadEnabled {
		if images, err := s.media.ProcessImages(ctx, ad.ID, photos); err == nil && len(images) > 0 {
			s.db.UpsertImages(ctx, images)
			ad.Images = images
		}
	}

	return ad, nil
}

func (s *Scraper) recheckOneAd(ctx context.Context, adID string) {
	existingAd, _ := s.db.GetAd(ctx, adID)

	ad, photos, err := s.fetchAndParseAd(ctx, adID)
	if err != nil {
		if strings.Contains(err.Error(), "404_NOT_FOUND") {
			log.Info().Str("ad_id", adID).Msg("ad deleted on platform, marking as deleted")
			if err := s.db.MarkAdDeleted(ctx, adID); err != nil {
				log.Warn().Err(err).Str("ad_id", adID).Msg("mark deleted failed")
			}
			if existingAd != nil {
				s.db.RecordHistory(ctx, adID, "ad_status", existingAd.AdStatus, "DELETED")
				s.db.RecordHistory(ctx, adID, "is_active", "true", "false")
			}
			s.cache.InvalidateAd(ctx, adID)
			return
		}
		log.Warn().Err(err).Str("ad_id", adID).Msg("recheck fetch failed")
		return
	}

	if existingAd != nil {
		s.trackChanges(ctx, existingAd, ad)
		ad.FirstSeenAt = existingAd.FirstSeenAt
		ad.TaskID = existingAd.TaskID
	}

	if err := s.db.UpsertAd(ctx, ad); err != nil {
		log.Warn().Err(err).Str("ad_id", adID).Msg("recheck upsert failed")
		return
	}
	s.snapBuf.Record(adID, ad.Views, ad.PriceEUR)
	s.cache.CacheAd(ctx, ad)
	s.cache.PublishAdUpdate(ctx, ad)

	if s.media != nil && len(photos) > 0 && s.liveConfig.Get().ImageUploadEnabled {
		if images, err := s.media.ProcessImages(ctx, ad.ID, photos); err == nil && len(images) > 0 {
			s.db.UpsertImages(ctx, images)
		}
	}
}

func (s *Scraper) trackChanges(ctx context.Context, old, new *kl.Ad) {
	if old.Price != new.Price {
		s.db.RecordHistory(ctx, old.ID, "price", fmt.Sprintf("%d", old.Price), fmt.Sprintf("%d", new.Price))
	}
	if old.Views != new.Views {
		s.db.RecordHistory(ctx, old.ID, "views", strconv.Itoa(old.Views), strconv.Itoa(new.Views))
	}
	if old.AdStatus != new.AdStatus {
		s.db.RecordHistory(ctx, old.ID, "ad_status", old.AdStatus, new.AdStatus)
	}
	if old.Title != new.Title {
		s.db.RecordHistory(ctx, old.ID, "title", old.Title, new.Title)
	}
	if old.Description != new.Description {
		s.db.RecordHistory(ctx, old.ID, "description", old.Description, new.Description)
	}
	if old.ContactName != new.ContactName {
		s.db.RecordHistory(ctx, old.ID, "contact_name", old.ContactName, new.ContactName)
	}
}

// ==================== CATEGORY SYNC ====================

func (s *Scraper) SyncCategories(ctx context.Context) error {
	log.Info().Msg("syncing categories from Kleinanzeigen API")

	body, err := s.doRequest(ctx, "https://api.kleinanzeigen.de/api/categories.json")
	if err != nil {
		return fmt.Errorf("fetch categories: %w", err)
	}

	cats, err := kl.ParseCategoriesResponse(body)
	if err != nil {
		return fmt.Errorf("parse categories: %w", err)
	}

	if err := s.db.UpsertCategoriesBatch(ctx, cats); err != nil {
		return fmt.Errorf("upsert categories: %w", err)
	}

	log.Info().Int("count", len(cats)).Msg("categories synced")
	return nil
}
