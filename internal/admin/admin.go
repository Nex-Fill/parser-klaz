package admin

import (
	"crypto/subtle"
	"embed"
	"encoding/json"
	"html/template"
	"net/http"
	"runtime"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"

	"github.com/danamakarenko/klaz-parser/internal/config"
	"github.com/danamakarenko/klaz-parser/internal/proxy"
	"github.com/danamakarenko/klaz-parser/internal/storage"
	"github.com/danamakarenko/klaz-parser/internal/task"
)

//go:embed templates/*.html
var templateFS embed.FS

type Handler struct {
	liveConfig *config.LiveConfig
	taskMgr    *task.Manager
	db         *storage.Postgres
	cache      *storage.Cache
	proxyPool  *proxy.Pool
	logBuf     *LogBuffer
	tmpl       *template.Template
	user       string
	pass       string
}

func NewHandler(lc *config.LiveConfig, tm *task.Manager, db *storage.Postgres, cache *storage.Cache, pp *proxy.Pool, lb *LogBuffer, adminUser, adminPass string) *Handler {
	funcMap := template.FuncMap{
		"mul": func(a, b float64) float64 { return a * b },
	}
	tmpl := template.Must(template.New("").Funcs(funcMap).ParseFS(templateFS, "templates/*.html"))
	return &Handler{
		liveConfig: lc, taskMgr: tm, db: db, cache: cache,
		proxyPool: pp, logBuf: lb, tmpl: tmpl,
		user: adminUser, pass: adminPass,
	}
}

func (h *Handler) Routes() chi.Router {
	r := chi.NewRouter()
	r.Use(h.basicAuth)

	r.Get("/", h.dashboard)
	r.Get("/settings", h.settings)
	r.Post("/settings", h.updateSettings)
	r.Get("/proxies", h.proxies)
	r.Get("/tasks", h.tasks)
	r.Get("/logs", h.logs)

	r.Post("/actions/recheck", h.forceRecheck)
	r.Post("/actions/category-sync", h.forceCategorySync)
	r.Post("/actions/proxy-reload", h.forceProxyReload)

	r.Get("/api/stats", h.statsJSON)
	return r
}

func (h *Handler) basicAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok ||
			subtle.ConstantTimeCompare([]byte(u), []byte(h.user)) != 1 ||
			subtle.ConstantTimeCompare([]byte(p), []byte(h.pass)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="Admin"`)
			http.Error(w, "Unauthorized", 401)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type dashData struct {
	TotalAds      int64
	ActiveAds     int64
	DeletedAds    int64
	AdsToday      int64
	ProxyAlive    int
	ProxyTotal    int
	RunningTasks  int
	Goroutines    int
	HeapMB        uint64
	SnapBufSize   int
	LastRecheck   string
	RedisCounters map[string]int64
}

func (h *Handler) dashboard(w http.ResponseWriter, r *http.Request) {
	stats, _ := h.db.GetDashboardStatsV2(r.Context())
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	d := dashData{
		ProxyAlive:    h.proxyPool.Count(),
		Goroutines:    runtime.NumGoroutine(),
		HeapMB:        ms.HeapAlloc / 1024 / 1024,
		RunningTasks:  len(h.taskMgr.ListTasks("")),
		RedisCounters: h.cache.GetStats(r.Context()),
	}
	if stats != nil {
		d.TotalAds = stats.TotalAds
		d.ActiveAds = stats.ActiveAds
		d.DeletedAds = stats.DeletedAds
		d.AdsToday = stats.AdsToday
	}

	h.tmpl.ExecuteTemplate(w, "dashboard.html", d)
}

func (h *Handler) settings(w http.ResponseWriter, r *http.Request) {
	h.tmpl.ExecuteTemplate(w, "settings.html", h.liveConfig.GetAll(r.Context()))
}

func (h *Handler) updateSettings(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	patch := map[string]interface{}{}

	boolFields := []string{"auto_recheck_enabled", "image_upload_enabled"}
	for _, key := range boolFields {
		patch[key] = r.FormValue(key) == "on" || r.FormValue(key) == "true"
	}

	for key, values := range r.Form {
		if len(values) == 0 {
			continue
		}
		val := values[0]
		switch key {
		case "auto_recheck_enabled", "image_upload_enabled":
			continue
		case "parse_workers", "image_workers", "free_tier_task_limit", "premium_tier_task_limit":
			var n int
			if err := json.Unmarshal([]byte(val), &n); err == nil {
				patch[key] = n
			}
		default:
			if val != "" {
				patch[key] = val
			}
		}
	}

	if err := h.liveConfig.Update(r.Context(), patch, "admin"); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	log.Info().Interface("patch", patch).Msg("admin updated settings")
	http.Redirect(w, r, "/admin/settings?saved=1", http.StatusSeeOther)
}

func (h *Handler) proxies(w http.ResponseWriter, r *http.Request) {
	h.tmpl.ExecuteTemplate(w, "proxies.html", h.proxyPool.Stats())
}

func (h *Handler) tasks(w http.ResponseWriter, r *http.Request) {
	h.tmpl.ExecuteTemplate(w, "tasks.html", h.taskMgr.ListTasks(""))
}

func (h *Handler) logs(w http.ResponseWriter, r *http.Request) {
	h.tmpl.ExecuteTemplate(w, "logs.html", h.logBuf.Entries())
}

func (h *Handler) forceRecheck(w http.ResponseWriter, r *http.Request) {
	h.taskMgr.TriggerFullRecheck()
	http.Redirect(w, r, "/admin/?action=recheck", http.StatusSeeOther)
}

func (h *Handler) forceCategorySync(w http.ResponseWriter, r *http.Request) {
	h.taskMgr.TriggerCategorySync()
	http.Redirect(w, r, "/admin/?action=category-sync", http.StatusSeeOther)
}

func (h *Handler) forceProxyReload(w http.ResponseWriter, r *http.Request) {
	h.proxyPool.Reload(r.Context())
	http.Redirect(w, r, "/admin/proxies?action=reload", http.StatusSeeOther)
}

func (h *Handler) statsJSON(w http.ResponseWriter, r *http.Request) {
	stats, _ := h.db.GetDashboardStatsV2(r.Context())
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	data := map[string]interface{}{
		"stats":      stats,
		"proxy":      h.proxyPool.Count(),
		"goroutines": runtime.NumGoroutine(),
		"heap_mb":    ms.HeapAlloc / 1024 / 1024,
		"tasks":      len(h.taskMgr.ListTasks("")),
		"counters":   h.cache.GetStats(r.Context()),
		"time":       time.Now().Format("15:04:05"),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}
