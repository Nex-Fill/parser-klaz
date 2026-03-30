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
	clientPool map[string]*http.Client
	clientMu   sync.Mutex
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
		media: media, pool: pool, filter: NewFilter(), snapBuf: snapBuf, clientPool: make(map[string]*http.Client),
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
	g.SetLimit(10)

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

		// Price-only snapshots removed — views=0 pollutes data.
		// Snapshots are recorded in batch counters with real views+favorites.

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
	isAuto := strings.HasPrefix(task.ID, "auto_")

	if isAuto {
		return s.processBatchFast(ctx, task, rawAds, catID)
	}
	return s.processBatchFull(ctx, task, rawAds, catID)
}

func (s *Scraper) processBatchFast(ctx context.Context, task *kl.ParseTask, rawAds []kl.RawAd, catID string) (found, checked int) {
	var ads []*kl.Ad
	for _, raw := range rawAds {
		if raw.Raw == nil {
			continue
		}
		ad, _ := kl.ParseAdFromSearchResult(raw.Raw)
		if ad == nil || ad.ID == "" {
			continue
		}
		ad.TaskID = task.ID
		if ad.CategoryID == "" {
			ad.CategoryID = catID
		}
		if !s.filter.Apply(ad, &task.Filters) {
			continue
		}
		ads = append(ads, ad)
		checked++
	}
	if len(ads) > 0 {
		if err := s.db.UpsertAdsFromSearch(ctx, ads); err != nil {
			log.Error().Err(err).Msg("fast batch upsert failed")
		}
		found = len(ads)
	}
	return
}

func (s *Scraper) processBatchFull(ctx context.Context, task *kl.ParseTask, rawAds []kl.RawAd, catID string) (found, checked int) {
	batchSize := s.cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 50
	}

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
				ad, photos, err := s.fetchAndParseAd(ctx, raw.ID)
				if err != nil {
					return
				}
				ad.TaskID = task.ID
				if ad.CategoryID == "" {
					ad.CategoryID = catID
				}
				if !s.filter.Apply(ad, &task.Filters) {
					return
				}
				var images []kl.AdImage
				if s.media != nil && len(photos) > 0 && s.liveConfig.Get().ImageUploadEnabled {
					images, _ = s.media.ProcessImages(ctx, ad.ID, photos)
				}
				s.snapBuf.RecordFull(ad.ID, ad.Views, ad.Favorites, ad.PriceEUR)

				mu.Lock()
				ads = append(ads, ad)
				allImages = append(allImages, images...)
				mu.Unlock()
			})
		}
		wg.Wait()
		checked += len(batch)

		if len(ads) > 0 {
			s.db.UpsertAdsBatch(ctx, ads)
			found += len(ads)
		}
		if len(allImages) > 0 {
			s.db.UpsertImages(ctx, allImages)
		}
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
	if proxyURL == "" {
		return &http.Client{Timeout: s.cfg.RequestTimeout}
	}
	s.clientMu.Lock()
	c, ok := s.clientPool[proxyURL]
	s.clientMu.Unlock()
	if ok {
		return c
	}

	transport := &http.Transport{
		MaxIdleConns: 100, MaxIdleConnsPerHost: 20, IdleConnTimeout: 90 * time.Second,
		DisableKeepAlives: false,
	}
	if u, err := url.Parse(proxyURL); err == nil {
		transport.Proxy = http.ProxyURL(u)
	}
	c = &http.Client{Transport: transport, Timeout: s.cfg.RequestTimeout}

	s.clientMu.Lock()
	s.clientPool[proxyURL] = c
	s.clientMu.Unlock()
	return c
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

		if tier.Name == "fresh" || tier.Name == "stale" {
			s.recheckBatchViewsOnly(ctx, ids)
		} else {
			s.recheckBatch(ctx, ids)
		}

		log.Info().
			Str("tier", tier.Name).
			Int("count", len(ids)).
			Dur("took", time.Since(start)).
			Msg("recheck tier done")
	}
	return nil
}

func (s *Scraper) recheckBatch(ctx context.Context, ids []string) {
	s.recheckBatchMode(ctx, ids, false)
}

func (s *Scraper) recheckBatchViewsOnly(ctx context.Context, ids []string) {
	s.recheckBatchMode(ctx, ids, true)
}

func (s *Scraper) recheckBatchMode(ctx context.Context, ids []string, viewsOnly bool) {
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
				if viewsOnly {
					s.recheckViewsOnly(ctx, id)
				} else {
					s.recheckOneAd(ctx, id)
				}
			})
		}
		wg.Wait()
	}
}

func (s *Scraper) recheckViewsOnly(ctx context.Context, adID string) {
	body, err := s.doRequest(ctx, kl.BuildViewsURL(adID))
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			s.db.MarkAdDeleted(ctx, adID)
			return
		}
		return
	}
	views := kl.ParseViewsResponse(body)
	if views > 0 {
		s.db.UpdateAdViews(ctx, adID, views)
		s.snapBuf.RecordFull(adID, views, 0, 0)
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

	ids := []string{adID}
	if body, err := s.doRequest(ctx, kl.BuildBatchViewsURL(ids)); err == nil {
		if vm := kl.ParseCountersResponse(body); vm[adID] > 0 {
			ad.Views = vm[adID]
		}
	}
	if body, err := s.doRequest(ctx, kl.BuildBatchFavoritesURL(ids)); err == nil {
		if fm := kl.ParseCountersResponse(body); fm[adID] > 0 {
			ad.Favorites = fm[adID]
		}
	}

	s.db.UpsertAd(ctx, ad)
	s.snapBuf.RecordFull(adID, ad.Views, ad.Favorites, ad.PriceEUR)
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
	s.snapBuf.RecordFull(adID, ad.Views, ad.Favorites, ad.PriceEUR)
	s.cache.CacheAd(ctx, ad)
	s.cache.PublishAdUpdate(ctx, ad)

	if s.media != nil && len(photos) > 0 && s.liveConfig.Get().ImageUploadEnabled {
		if images, err := s.media.ProcessImages(ctx, ad.ID, photos); err == nil && len(images) > 0 {
			s.db.UpsertImages(ctx, images)
		}
	}
}

func (s *Scraper) trackChanges(ctx context.Context, old, new *kl.Ad) {
	if old.Price != new.Price && old.Price > 0 && new.Price > 0 {
		s.db.RecordHistory(ctx, old.ID, "price", fmt.Sprintf("%d", old.Price), fmt.Sprintf("%d", new.Price))
	}
	if old.AdStatus != new.AdStatus && new.AdStatus != "" {
		s.db.RecordHistory(ctx, old.ID, "ad_status", old.AdStatus, new.AdStatus)
		now := time.Now()
		s.db.SetStatusChangedAt(ctx, old.ID, now)

		switch new.AdStatus {
		case "ACTIVE":
			new.IsActive = true
			new.IsDeleted = false
		case "PAUSED", "RESERVED":
			new.IsActive = false
		case "SOLD", "CLOSED", "DELETED":
			new.IsActive = false
			new.IsDeleted = true
			new.DeletedAt = &now
		}
	}
	if old.Title != new.Title && old.Title != "" && new.Title != "" {
		s.db.RecordHistory(ctx, old.ID, "title", old.Title, new.Title)
	}
	if normalizeWS(old.Description) != normalizeWS(new.Description) && old.Description != "" && new.Description != "" {
		s.db.RecordHistory(ctx, old.ID, "description", old.Description, new.Description)
	}
	if old.PosterType != new.PosterType && old.PosterType != "" && new.PosterType != "" {
		s.db.RecordHistory(ctx, old.ID, "poster_type", old.PosterType, new.PosterType)
	}
	if old.PriceType != new.PriceType && new.PriceType != "" {
		s.db.RecordHistory(ctx, old.ID, "price_type", old.PriceType, new.PriceType)
	}
	if old.BuyNowSelected != new.BuyNowSelected {
		s.db.RecordHistory(ctx, old.ID, "buy_now_selected", fmt.Sprintf("%v", old.BuyNowSelected), fmt.Sprintf("%v", new.BuyNowSelected))
	}
}

func normalizeWS(s string) string {
	prev := byte(' ')
	buf := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			if prev != ' ' {
				buf = append(buf, ' ')
			}
			prev = ' '
		} else {
			buf = append(buf, c)
			prev = c
		}
	}
	return string(buf)
}

func (s *Scraper) FetchCategoryMetadata(ctx context.Context, categoryID string) (map[string]interface{}, error) {
	url := fmt.Sprintf("https://api.kleinanzeigen.de/api/ads/metadata/%s.json", categoryID)
	body, err := s.doRequest(ctx, url)
	if err != nil {
		return nil, err
	}
	var result map[string]interface{}
	json.Unmarshal(body, &result)
	return result, nil
}

// ==================== IMAGE LOADER ====================

func (s *Scraper) LoadMissingImages(ctx context.Context, batchSize int) (int, error) {
	if s.media == nil || !s.liveConfig.Get().ImageUploadEnabled {
		return 0, nil
	}

	ids, err := s.db.GetAdsWithoutImages(ctx, batchSize)
	if err != nil || len(ids) == 0 {
		return 0, err
	}

	log.Info().Int("count", len(ids)).Msg("loading missing images")

	var wg sync.WaitGroup
	for _, id := range ids {
		id := id
		wg.Add(1)
		s.pool.Submit(func() {
			defer wg.Done()

			body, err := s.doRequest(ctx, kl.BuildAdURL(id))
			if err != nil {
				return
			}
			_, photos, err := kl.ParseAdResponse(body)
			if err != nil || len(photos) == 0 {
				return
			}

			images, err := s.media.ProcessImages(ctx, id, photos)
			if err == nil && len(images) > 0 {
				s.db.UpsertImages(ctx, images)
			}
		})
	}
	wg.Wait()

	log.Info().Int("processed", len(ids)).Msg("missing images loaded")
	return len(ids), nil
}

// ==================== SYNC CATEGORY ATTRIBUTES ====================

func (s *Scraper) SyncCategoryAttributes(ctx context.Context) error {
	cats, _ := s.db.GetAllCategories(ctx)
	if len(cats) == 0 {
		return nil
	}

	log.Info().Int("categories", len(cats)).Msg("syncing category attributes")
	var synced int

	for _, cat := range cats {
		if cat.HasChildren {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		url := fmt.Sprintf("https://api.kleinanzeigen.de/api/ads/metadata/%s.json", cat.ID)
		body, err := s.doRequest(ctx, url)
		if err != nil {
			continue
		}

		var resp map[string]interface{}
		if json.Unmarshal(body, &resp) != nil {
			continue
		}

		for _, v := range resp {
			vm, ok := v.(map[string]interface{})
			if !ok {
				continue
			}
			inner, ok := vm["value"].(map[string]interface{})
			if !ok {
				continue
			}
			attrs, ok := inner["attributes"].(map[string]interface{})
			if !ok {
				continue
			}
			attrList, ok := attrs["attribute"].([]interface{})
			if !ok {
				continue
			}

			for _, a := range attrList {
				attr, ok := a.(map[string]interface{})
				if !ok {
					continue
				}
				name, _ := attr["name"].(string)
				label, _ := attr["localized-label"].(string)
				values, _ := attr["value"].([]interface{})

				var valList []map[string]string
				if values != nil {
					for _, vv := range values {
						if vm, ok := vv.(map[string]interface{}); ok {
							entry := map[string]string{}
							if l, ok := vm["localized-label"].(string); ok {
								entry["label"] = l
							}
							if v, ok := vm["value"].(string); ok {
								entry["value"] = v
							}
							valList = append(valList, entry)
						}
					}
				}

				valJSON, _ := json.Marshal(valList)
				s.db.UpsertCategoryAttribute(ctx, cat.ID, name, label, valJSON)
				synced++
			}
		}
	}

	log.Info().Int("attributes", synced).Msg("category attributes synced")
	return nil
}

// ==================== DEEP SCAN WITH PRICE SPLITTING ====================

func (s *Scraper) DeepScanAll(ctx context.Context) int {
	cats, _ := s.db.GetAllCategories(ctx)
	if len(cats) == 0 {
		return 0
	}

	var leafCats []string
	for _, c := range cats {
		if !c.HasChildren {
			leafCats = append(leafCats, c.ID)
		}
	}

	priceBuckets := []struct{ min, max int }{
		{0, 5}, {5, 10}, {10, 20}, {20, 50},
		{50, 100}, {100, 200}, {200, 500},
		{500, 1000}, {1000, 2000}, {2000, 5000},
		{5000, 10000}, {10000, 50000}, {50000, 0},
	}

	log.Info().Int("categories", len(leafCats)).Int("price_buckets", len(priceBuckets)).Msg("deep scan: starting")
	start := time.Now()
	var totalAds int

	for _, catID := range leafCats {
		select {
		case <-ctx.Done():
			return totalAds
		default:
		}

		for _, bucket := range priceBuckets {
			for page := 0; page < 100; page++ {
				params := kl.SearchParams{
					CategoryID: catID,
					Page:       page,
					Size:       100,
				}
				if bucket.min > 0 {
					params.MinPrice = &bucket.min
				}
				if bucket.max > 0 {
					params.MaxPrice = &bucket.max
				}

				result, err := s.searchAds(ctx, params)
				if err != nil || len(result.Ads) == 0 {
					break
				}

				var ads []*kl.Ad
				for _, raw := range result.Ads {
					if raw.Raw == nil {
						continue
					}
					ad, _ := kl.ParseAdFromSearchResult(raw.Raw)
					if ad == nil || ad.ID == "" {
						continue
					}
					if ad.CategoryID == "" {
						ad.CategoryID = catID
					}
					ads = append(ads, ad)
				}

				if len(ads) > 0 {
					s.db.UpsertAdsFromSearch(ctx, ads)
					totalAds += len(ads)
				}

				if len(result.Ads) < 100 {
					break
				}
			}
		}
	}

	log.Info().
		Int("total_ads", totalAds).
		Dur("took", time.Since(start)).
		Msg("deep scan: complete")

	return totalAds
}

// ==================== NEW ADS WATCHER (global feed) ====================

func (s *Scraper) ScanNewAds(ctx context.Context) int {
	var totalNew int
	const maxPages = 100

	for page := 0; page < maxPages; page++ {
		select {
		case <-ctx.Done():
			return totalNew
		default:
		}

		params := kl.SearchParams{
			Page: page, Size: 100,
		}
		result, err := s.searchAds(ctx, params)
		if err != nil || len(result.Ads) == 0 {
			break
		}

		var ids []string
		var ads []*kl.Ad
		for _, raw := range result.Ads {
			if raw.Raw == nil {
				continue
			}
			ad, _ := kl.ParseAdFromSearchResult(raw.Raw)
			if ad == nil || ad.ID == "" {
				continue
			}
			ids = append(ids, ad.ID)
			ads = append(ads, ad)
		}

		existing, _ := s.db.CountExistingIDs(ctx, ids)
		newOnPage := len(ids) - existing

		if len(ads) > 0 {
			s.db.UpsertAdsFromSearch(ctx, ads)
			totalNew += newOnPage
		}

		if len(ids) > 0 && float64(existing)/float64(len(ids)) > 0.9 {
			break
		}

		if len(result.Ads) < 100 {
			break
		}
	}
	return totalNew
}

// ==================== BATCH COUNTERS (views + favorites) ====================

func (s *Scraper) BatchCountersUpdateTier(ctx context.Context, tier string) error {
	ids, err := s.db.GetAdIDsByTier(ctx, tier)
	if err != nil {
		return fmt.Errorf("get active IDs: %w", err)
	}
	if len(ids) == 0 {
		return nil
	}

	log.Info().Int("total_ads", len(ids)).Str("tier", tier).Msg("batch counters: starting")
	start := time.Now()

	const batchSize = 75
	var totalViews, totalFav int
	var mu sync.Mutex

	for i := 0; i < len(ids); i += batchSize * 50 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		chunkEnd := i + batchSize*50
		if chunkEnd > len(ids) {
			chunkEnd = len(ids)
		}
		megaChunk := ids[i:chunkEnd]

		var wg sync.WaitGroup
		for j := 0; j < len(megaChunk); j += batchSize {
			bEnd := j + batchSize
			if bEnd > len(megaChunk) {
				bEnd = len(megaChunk)
			}
			batch := megaChunk[j:bEnd]

			wg.Add(1)
			s.pool.Submit(func() {
				defer wg.Done()

				var viewsMap, favMap map[string]int
				var vwg sync.WaitGroup

				vwg.Add(2)
				go func() {
					defer vwg.Done()
					body, err := s.doRequest(ctx, kl.BuildBatchViewsURL(batch))
					if err == nil {
						viewsMap = kl.ParseCountersResponse(body)
					}
				}()
				go func() {
					defer vwg.Done()
					body, err := s.doRequest(ctx, kl.BuildBatchFavoritesURL(batch))
					if err == nil {
						favMap = kl.ParseCountersResponse(body)
					}
				}()
				vwg.Wait()

				bothFailed := viewsMap == nil && favMap == nil
				if viewsMap == nil {
					viewsMap = make(map[string]int)
				}
				if favMap == nil {
					favMap = make(map[string]int)
				}

				if bothFailed {
					return
				}

				for _, id := range batch {
					v := viewsMap[id]
					f := favMap[id]
					if v > 0 || f > 0 {
						s.snapBuf.RecordFull(id, v, f, 0)
					}
				}

				if err := s.db.BatchUpdateCounters(ctx, viewsMap, favMap); err != nil {
					log.Warn().Err(err).Msg("batch counters DB update failed")
				}

				mu.Lock()
				totalViews += len(viewsMap)
				totalFav += len(favMap)
				mu.Unlock()
			})
		}
		wg.Wait()
	}

	log.Info().
		Int("total_ads", len(ids)).
		Int("views_updated", totalViews).
		Int("favorites_updated", totalFav).
		Str("tier", tier).
		Dur("took", time.Since(start)).
		Msg("batch counters: complete")

	return nil
}

// ==================== DEBUG ====================

func (s *Scraper) DebugRawFetch(ctx context.Context, adID string) map[string]interface{} {
	result := map[string]interface{}{}

	if body, err := s.doRequest(ctx, kl.BuildAdURL(adID)); err == nil {
		var raw interface{}
		json.Unmarshal(body, &raw)
		result["ad_detail"] = raw
	} else {
		result["ad_detail_error"] = err.Error()
	}

	if body, err := s.doRequest(ctx, kl.BuildViewsURL(adID)); err == nil {
		var raw interface{}
		json.Unmarshal(body, &raw)
		result["views_old"] = raw
	} else {
		result["views_old_error"] = err.Error()
	}

	vipURL := fmt.Sprintf("https://api.kleinanzeigen.de/api/v2/counters/ads/vip?adIds=%s", adID)
	if body, err := s.doRequest(ctx, vipURL); err == nil {
		var raw interface{}
		json.Unmarshal(body, &raw)
		result["views_v2_vip"] = raw
		result["views_v2_vip_raw"] = string(body)
	} else {
		result["views_v2_vip_error"] = err.Error()
	}

	watchURL := fmt.Sprintf("https://api.kleinanzeigen.de/api/v2/counters/ads/watchlist?adIds=%s", adID)
	if body, err := s.doRequest(ctx, watchURL); err == nil {
		var raw interface{}
		json.Unmarshal(body, &raw)
		result["favorites_v2_watchlist"] = raw
		result["favorites_v2_watchlist_raw"] = string(body)
	} else {
		result["favorites_v2_watchlist_error"] = err.Error()
	}

	return result
}

func (s *Scraper) DebugBatchCounters(ctx context.Context, adIDs []string) map[string]interface{} {
	result := map[string]interface{}{"ad_count": len(adIDs)}
	ids := strings.Join(adIDs, ",")

	vipURL := fmt.Sprintf("https://api.kleinanzeigen.de/api/v2/counters/ads/vip?adIds=%s", ids)
	if body, err := s.doRequest(ctx, vipURL); err == nil {
		var raw interface{}
		json.Unmarshal(body, &raw)
		result["vip"] = raw
		result["vip_raw"] = string(body)
		result["vip_size"] = len(body)
	} else {
		result["vip_error"] = err.Error()
	}

	watchURL := fmt.Sprintf("https://api.kleinanzeigen.de/api/v2/counters/ads/watchlist?adIds=%s", ids)
	if body, err := s.doRequest(ctx, watchURL); err == nil {
		var raw interface{}
		json.Unmarshal(body, &raw)
		result["watchlist"] = raw
		result["watchlist_raw"] = string(body)
		result["watchlist_size"] = len(body)
	} else {
		result["watchlist_error"] = err.Error()
	}

	return result
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
