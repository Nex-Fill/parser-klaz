package task

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/danamakarenko/klaz-parser/internal/parser"
	"github.com/danamakarenko/klaz-parser/internal/storage"
	kl "github.com/danamakarenko/klaz-parser/pkg/kleinanzeigen"
)

type Manager struct {
	db      *storage.Postgres
	cache   *storage.Cache
	scraper *parser.Scraper

	tasks   map[string]*kl.ParseTask
	cancels map[string]context.CancelFunc
	mu      sync.RWMutex

	recheckCancel    context.CancelFunc
	parseLock        sync.Mutex
	countersRunning  atomic.Bool
}

func (m *Manager) Scraper() *parser.Scraper { return m.scraper }

func NewManager(db *storage.Postgres, cache *storage.Cache, scraper *parser.Scraper) *Manager {
	return &Manager{
		db:      db,
		cache:   cache,
		scraper: scraper,
		tasks:   make(map[string]*kl.ParseTask),
		cancels: make(map[string]context.CancelFunc),
	}
}

// ==================== BACKGROUND LOOPS ====================

// StartBatchCountersLoop runs batch views+favorites update every 45 minutes.
// Uses /api/v2/counters/ads/vip + /watchlist — 50 ads per request, ~3500 ads/sec.
// Also detects deleted ads (missing from counters response).
func (m *Manager) StartBatchCountersLoop(ctx context.Context) {
	ctx, m.recheckCancel = context.WithCancel(ctx)

	go func() {
		time.Sleep(20 * time.Second)
		m.countersRunning.Store(true)
		log.Info().Msg("running initial batch counters update")
		if err := m.scraper.BatchCountersUpdate(ctx); err != nil {
			log.Error().Err(err).Msg("batch counters failed")
		}
		m.countersRunning.Store(false)

		ticker := time.NewTicker(45 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.countersRunning.Store(true)
				log.Info().Msg("batch counters cycle")
				if err := m.scraper.BatchCountersUpdate(ctx); err != nil {
					log.Error().Err(err).Msg("batch counters failed")
				}
				m.countersRunning.Store(false)
			}
		}
	}()
	log.Info().Msg("batch counters loop started (every 45 min, views+favorites+deleted)")
}

// StartAutoParseLoop continuously parses ALL categories for new ads.
// First run: deep scan (50 pages per category) to fill the database.
// Subsequent runs: shallow scan (1 page) to catch only new ads.
// Cycle: parse → wait 10 min → repeat.
func (m *Manager) StartAutoParseLoop(ctx context.Context) {
	go func() {
		time.Sleep(15 * time.Second)
		adCount, _ := m.db.GetAdCount(ctx)
		firstRun := adCount < 100000

		// Deep scan disabled — enough ads collected, focus on counters updates
		// To trigger manually: POST /api/ads/deep-scan

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			cats, err := m.db.GetAllCategories(ctx)
			if err != nil || len(cats) == 0 {
				log.Warn().Err(err).Msg("auto-parse: no categories, waiting")
				time.Sleep(5 * time.Minute)
				continue
			}

			var catURLs []string
			for _, cat := range cats {
				if cat.HasChildren {
					continue
				}
				catURLs = append(catURLs, "https://www.kleinanzeigen.de/s-cat/"+cat.ID)
			}

			maxPages := 2
			label := "shallow"
			if firstRun {
				maxPages = 100
				label = "DEEP (first run)"
			}

			log.Info().
				Int("categories", len(catURLs)).
				Int("max_pages", maxPages).
				Str("mode", label).
				Msg("auto-parse: starting")

			m.parseLock.Lock()
			start := time.Now()

			task := &kl.ParseTask{
				ID:                  fmt.Sprintf("auto_%d", time.Now().UnixMilli()),
				Name:                "Auto Parse " + label,
				Status:              kl.TaskRunning,
				CategoryURLs:        catURLs,
				MaxPagesPerCategory: maxPages,
				Progress:            kl.TaskProgress{TotalCategories: len(catURLs)},
				CreatedAt:           time.Now(),
				UpdatedAt:           time.Now(),
			}

			m.scraper.RunTask(ctx, task)

			m.parseLock.Unlock()

			log.Info().
				Dur("took", time.Since(start)).
				Int("found", task.AdsCount).
				Str("mode", label).
				Msg("auto-parse: cycle complete")

			firstRun = false

			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Minute):
			}
		}
	}()
	log.Info().Msg("auto-parse loop started (deep first, then shallow every 2 min)")
}

// StartNewAdsWatcher polls the global feed (no category filter) every 30 seconds
// to catch brand new ads across ALL categories instantly.
func (m *Manager) StartNewAdsWatcher(ctx context.Context) {
	go func() {
		time.Sleep(25 * time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			start := time.Now()
			count := m.scraper.ScanNewAds(ctx)

			if count > 0 {
				log.Info().Int("new_ads", count).Dur("took", time.Since(start)).Msg("new ads watcher: found")
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(30 * time.Second):
			}
		}
	}()
	log.Info().Msg("new ads watcher started (every 30 sec, global feed)")
}

func (m *Manager) StartImageLoaderLoop(ctx context.Context) {
	go func() {
		time.Sleep(30 * time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if m.countersRunning.Load() {
				time.Sleep(5 * time.Second)
				continue
			}
			count, _ := m.scraper.LoadMissingImages(ctx, 5000)
			if count == 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Minute):
				}
			}
		}
	}()
	log.Info().Msg("image loader loop started (continuous, 5000 ads/batch)")
}

func (m *Manager) StartMetricsRefreshLoop(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(30 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.db.RefreshSellerAdCounts(ctx)
			}
		}
	}()
	log.Info().Msg("seller ad count refresh started (every 30 min)")
}

func (m *Manager) StartCategorySyncLoop(ctx context.Context) {
	go func() {
		if err := m.scraper.SyncCategories(ctx); err != nil {
			log.Warn().Err(err).Msg("initial category sync failed")
		}
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.scraper.SyncCategories(ctx)
			}
		}
	}()
}

// ==================== SAVED FILTER NOTIFICATIONS ====================

func (m *Manager) StartFilterNotificationLoop(ctx context.Context) {
	go func() {
		time.Sleep(2 * time.Minute)
		lastCheck := time.Now()
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.checkSavedFilters(ctx, lastCheck)
				lastCheck = time.Now()
			}
		}
	}()
	log.Info().Msg("filter notification loop started (every 10 min)")
}

func (m *Manager) checkSavedFilters(ctx context.Context, since time.Time) {
	filters, err := m.db.GetActiveNotifyFilters(ctx)
	if err != nil || len(filters) == 0 {
		return
	}

	for _, sf := range filters {
		if sf.NotifyOnNew {
			newAds, err := m.db.GetNewAdsSince(ctx, sf.CategoryIDs, since)
			if err != nil || len(newAds) == 0 {
				continue
			}
			title := fmt.Sprintf("%d new ads matching \"%s\"", len(newAds), sf.Name)
			body := ""
			for i, ad := range newAds {
				if i >= 5 {
					body += fmt.Sprintf("... and %d more", len(newAds)-5)
					break
				}
				body += fmt.Sprintf("• %s — €%.0f\n", ad.Title, ad.PriceEUR)
			}
			n := &kl.Notification{
				UserID: sf.UserID,
				Type:   "new_ads",
				Title:  title,
				Body:   body,
				Data: map[string]interface{}{
					"filter_id": sf.ID,
					"count":     len(newAds),
				},
			}
			m.db.CreateNotification(ctx, n)
		}

		if sf.NotifyOnPriceDrop {
			drops, err := m.db.GetRecentPriceDrops(ctx, sf.CategoryIDs, since)
			if err != nil || len(drops) == 0 {
				continue
			}
			title := fmt.Sprintf("%d price drops matching \"%s\"", len(drops), sf.Name)
			body := ""
			for i, ad := range drops {
				if i >= 5 {
					body += fmt.Sprintf("... and %d more", len(drops)-5)
					break
				}
				body += fmt.Sprintf("• %s — €%.0f\n", ad.Title, ad.PriceEUR)
			}
			n := &kl.Notification{
				UserID: sf.UserID,
				Type:   "price_drop",
				Title:  title,
				Body:   body,
				Data: map[string]interface{}{
					"filter_id": sf.ID,
					"count":     len(drops),
				},
			}
			m.db.CreateNotification(ctx, n)
		}
	}
}

// ==================== INSTANT RECHECK (user-triggered) ====================

func (m *Manager) InstantRecheck(ctx context.Context, adID string) (*kl.Ad, error) {
	return m.scraper.InstantRecheck(ctx, adID)
}

func (m *Manager) TriggerFullRecheck() {
	go func() {
		log.Info().Msg("manual full recheck triggered")
		ctx := context.Background()
		if err := m.scraper.RecheckByPriority(ctx); err != nil {
			log.Error().Err(err).Msg("manual recheck failed")
		}
	}()
}

func (m *Manager) TriggerCategorySync() {
	go func() {
		log.Info().Msg("category sync triggered")
		ctx := context.Background()
		if err := m.scraper.SyncCategories(ctx); err != nil {
			log.Error().Err(err).Msg("category sync failed")
		}
	}()
}

// ==================== TASK MANAGEMENT ====================

func (m *Manager) CreateTask(ctx context.Context, req CreateTaskRequest) (*kl.ParseTask, error) {
	taskID := fmt.Sprintf("task_%d_%d", time.Now().UnixMilli(), len(m.tasks)+1)

	task := &kl.ParseTask{
		ID:                  taskID,
		Name:                req.Name,
		Status:              kl.TaskPending,
		CategoryURLs:        req.CategoryURLs,
		Filters:             req.Filters,
		MaxPagesPerCategory: req.MaxPagesPerCategory,
		MaxAdsToCheck:       req.MaxAdsToCheck,
		MonitorHours:        req.MonitorHours,
		UserID:              req.UserID,
		Progress: kl.TaskProgress{
			TotalCategories: len(req.CategoryURLs),
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if err := m.db.SaveTask(ctx, task); err != nil {
		return nil, fmt.Errorf("save task: %w", err)
	}

	m.mu.Lock()
	m.tasks[taskID] = task
	m.mu.Unlock()

	taskCtx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	m.cancels[taskID] = cancel
	m.mu.Unlock()

	go func() {
		if task.MonitorHours != nil && *task.MonitorHours > 0 {
			m.runMonitoringTask(taskCtx, task)
		} else {
			m.runTask(taskCtx, task)
		}
	}()

	return task, nil
}

func (m *Manager) runTask(ctx context.Context, task *kl.ParseTask) {
	if err := m.scraper.RunTask(ctx, task); err != nil {
		log.Error().Err(err).Str("task_id", task.ID).Msg("task failed")
	}
}

func (m *Manager) runMonitoringTask(ctx context.Context, task *kl.ParseTask) {
	hours := *task.MonitorHours
	deadline := time.Now().Add(time.Duration(hours) * time.Hour)
	cycle := 0

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if task.Status == kl.TaskStopped {
			return
		}

		cycle++
		task.Progress.MonitorCycles = cycle
		log.Info().Str("task_id", task.ID).Int("cycle", cycle).Msg("monitor cycle")

		if err := m.scraper.RunTask(ctx, task); err != nil {
			log.Warn().Err(err).Msg("monitor cycle error")
		}
		task.Status = kl.TaskRunning

		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Minute):
		}
	}

	task.Status = kl.TaskCompleted
	now := time.Now()
	task.CompletedAt = &now
	task.UpdatedAt = now
	m.db.SaveTask(context.Background(), task)
}

func (m *Manager) GetTask(taskID string) *kl.ParseTask {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tasks[taskID]
}

func (m *Manager) GetTaskFromCache(ctx context.Context, taskID string) (*kl.ParseTask, error) {
	if task, err := m.cache.GetTaskProgress(ctx, taskID); err == nil && task != nil {
		return task, nil
	}
	m.mu.RLock()
	task, ok := m.tasks[taskID]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("task %s not found", taskID)
	}
	return task, nil
}

func (m *Manager) ListTasks(userID string) []*kl.ParseTask {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*kl.ParseTask
	for _, task := range m.tasks {
		if userID == "" || task.UserID == userID {
			result = append(result, task)
		}
	}
	return result
}

func (m *Manager) PauseTask(taskID string) error {
	m.mu.RLock()
	task, ok := m.tasks[taskID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}
	if task.Status != kl.TaskRunning {
		return fmt.Errorf("task is not running")
	}
	task.Status = kl.TaskPaused
	task.UpdatedAt = time.Now()
	return nil
}

func (m *Manager) ResumeTask(taskID string) error {
	m.mu.RLock()
	task, ok := m.tasks[taskID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}
	if task.Status != kl.TaskPaused {
		return fmt.Errorf("task is not paused")
	}
	task.Status = kl.TaskRunning
	task.UpdatedAt = time.Now()
	return nil
}

func (m *Manager) StopTask(taskID string) error {
	m.mu.Lock()
	task, ok := m.tasks[taskID]
	cancel := m.cancels[taskID]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}
	task.Status = kl.TaskStopped
	task.UpdatedAt = time.Now()
	if cancel != nil {
		cancel()
	}
	return nil
}

func (m *Manager) DeleteTask(taskID string) error {
	m.StopTask(taskID)
	m.mu.Lock()
	delete(m.tasks, taskID)
	delete(m.cancels, taskID)
	m.mu.Unlock()
	return nil
}

func (m *Manager) Shutdown() {
	if m.recheckCancel != nil {
		m.recheckCancel()
	}
	m.mu.RLock()
	for id, cancel := range m.cancels {
		log.Info().Str("task_id", id).Msg("stopping task")
		cancel()
	}
	m.mu.RUnlock()
}

type CreateTaskRequest struct {
	Name                string          `json:"task_name"`
	CategoryURLs        []string        `json:"category_urls"`
	Filters             kl.ParseFilters `json:"filters"`
	MaxPagesPerCategory int             `json:"max_pages_per_category"`
	MaxAdsToCheck       *int            `json:"max_ads_to_check,omitempty"`
	MonitorHours        *int            `json:"monitor_hours,omitempty"`
	UserID              string          `json:"user_id,omitempty"`
}
