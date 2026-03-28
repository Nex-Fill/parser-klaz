package api

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/go-chi/httprate"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"

	"github.com/danamakarenko/klaz-parser/internal/proxy"
	"github.com/danamakarenko/klaz-parser/internal/storage"
	"github.com/danamakarenko/klaz-parser/internal/task"
	kl "github.com/danamakarenko/klaz-parser/pkg/kleinanzeigen"
)

type Server struct {
	taskMgr   *task.Manager
	db        *storage.Postgres
	cache     *storage.Cache
	proxyPool *proxy.Pool
	jwtSecret []byte
	wsHub     *WSHub
}

func NewServer(tm *task.Manager, db *storage.Postgres, cache *storage.Cache, pp *proxy.Pool, jwtSecret string) *Server {
	secret := []byte(jwtSecret)
	hub := NewWSHub(cache.Client(), secret)
	return &Server{taskMgr: tm, db: db, cache: cache, proxyPool: pp, jwtSecret: secret, wsHub: hub}
}

func (s *Server) StartWSHub(ctx context.Context) {
	go s.wsHub.Run(ctx)
}

type ctxKey string

const ctxUserID ctxKey = "user_id"

func getUserID(r *http.Request) (string, bool) {
	uid, ok := r.Context().Value(ctxUserID).(string)
	return uid, ok && uid != ""
}

func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.RealIP, middleware.Recoverer)
	r.Use(middleware.Compress(5))
	r.Use(middleware.Timeout(30 * time.Second))
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins: []string{"*"}, AllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders: []string{"*"}, AllowCredentials: true,
	}))
	r.Use(httprate.LimitByIP(200, time.Minute))

	r.Get("/health", s.health)

	r.Get("/api/ws", s.wsHub.HandleWS)

	r.Route("/api", func(r chi.Router) {
		r.Post("/auth/register", s.register)
		r.Post("/auth/login", s.login)

		r.Route("/categories", func(r chi.Router) {
			r.Get("/", s.getCategories)
			r.Get("/tree", s.getCategoryTree)
			r.Get("/search", s.searchCategories)
			r.Post("/sync", s.syncCategories)
		})

		r.Group(func(r chi.Router) {
			r.Use(s.authMiddleware)

			r.Get("/me", s.me)

			r.Route("/tasks", func(r chi.Router) {
				r.Post("/", s.createTask)
				r.Get("/", s.listTasks)
				r.Get("/{taskID}", s.getTask)
				r.Get("/{taskID}/ads", s.getTaskAds)
				r.Post("/{taskID}/pause", s.pauseTask)
				r.Post("/{taskID}/resume", s.resumeTask)
				r.Post("/{taskID}/stop", s.stopTask)
				r.Delete("/{taskID}", s.deleteTask)
			})

			r.Route("/ads", func(r chi.Router) {
				r.Post("/search", s.searchAdsAdvanced)
				r.Post("/export", s.exportAds)
				r.Post("/batch", s.batchGetAds)
				r.Get("/{adID}", s.getAd)
				r.Post("/{adID}/recheck", s.recheckAd)
				r.Get("/{adID}/history", s.getAdHistory)
				r.Get("/{adID}/statistics", s.getAdStatistics)
				r.Post("/recheck-all", s.recheckAll)
			})

			r.Route("/sellers", func(r chi.Router) {
				r.Get("/{sellerID}", s.getSellerProfile)
				r.Get("/{sellerID}/ads", s.getSellerAds)
			})

			r.Route("/notifications", func(r chi.Router) {
				r.Get("/", s.getNotifications)
				r.Post("/{notifID}/read", s.markNotificationRead)
				r.Post("/read-all", s.markAllNotificationsRead)
			})

			r.Route("/filters", func(r chi.Router) {
				r.Get("/", s.getSavedFilters)
				r.Post("/", s.createSavedFilter)
				r.Delete("/{filterID}", s.deleteSavedFilter)
			})

			r.Get("/dashboard", s.dashboard)
			r.Get("/debug/raw/{adID}", s.debugRawFetch)
			r.Get("/debug/batch-counters", s.debugBatchCounters)

			r.Route("/proxy", func(r chi.Router) {
				r.Use(s.adminOnly)
				r.Get("/stats", s.proxyStats)
			})
		})
	})

	return r
}

// ==================== AUTH ====================

func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if len(auth) < 8 || auth[:7] != "Bearer " {
			respondError(w, 401, "unauthorized")
			return
		}
		token, err := jwt.Parse(auth[7:], func(t *jwt.Token) (interface{}, error) {
			return s.jwtSecret, nil
		})
		if err != nil || !token.Valid {
			respondError(w, 401, "invalid token")
			return
		}
		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			respondError(w, 401, "invalid claims")
			return
		}
		userID, _ := claims["sub"].(string)
		if userID == "" {
			respondError(w, 401, "invalid token: no sub")
			return
		}
		ctx := context.WithValue(r.Context(), ctxUserID, userID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) adminOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID, ok := getUserID(r)
		if !ok {
			respondError(w, 401, "unauthorized")
			return
		}
		user, err := s.db.GetUserByID(r.Context(), userID)
		if err != nil || !user.IsAdmin {
			respondError(w, 403, "admin access required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) register(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
		Name     string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Email == "" || req.Password == "" {
		respondError(w, 400, "email and password required")
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		respondError(w, 500, "password hash failed")
		return
	}
	user := &kl.User{
		ID: kl.GenerateSessionID(), Email: req.Email,
		PasswordHash: string(hash), Name: req.Name,
		CreatedAt: time.Now(),
	}
	if err := s.db.CreateUser(r.Context(), user); err != nil {
		respondError(w, 409, "user already exists")
		return
	}
	token := s.generateJWT(user.ID, user.Email, user.Role, user.IsAdmin)
	respond(w, 201, map[string]interface{}{"token": token, "user": user})
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, 400, "invalid body")
		return
	}
	user, err := s.db.GetUserByEmail(r.Context(), req.Email)
	if err != nil {
		respondError(w, 401, "invalid credentials")
		return
	}
	if bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)) != nil {
		respondError(w, 401, "invalid credentials")
		return
	}
	s.db.UpdateLastLogin(r.Context(), user.ID)
	token := s.generateJWT(user.ID, user.Email, user.Role, user.IsAdmin)
	respond(w, 200, map[string]interface{}{"token": token, "user": user})
}

func (s *Server) me(w http.ResponseWriter, r *http.Request) {
	userID, ok := getUserID(r)
	if !ok {
		respondError(w, 401, "unauthorized")
		return
	}
	user, err := s.db.GetUserByID(r.Context(), userID)
	if err != nil {
		respondError(w, 404, "user not found")
		return
	}
	respond(w, 200, map[string]interface{}{"user": user})
}

func (s *Server) generateJWT(userID, email, role string, isAdmin bool) string {
	claims := jwt.MapClaims{
		"sub":   userID,
		"email": email,
		"role":  role,
		"admin": isAdmin,
		"exp":   time.Now().Add(7 * 24 * time.Hour).Unix(),
	}
	token, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.jwtSecret)
	return token
}

// ==================== TASKS ====================

func (s *Server) createTask(w http.ResponseWriter, r *http.Request) {
	userID, ok := getUserID(r)
	if !ok {
		respondError(w, 401, "unauthorized")
		return
	}
	var req task.CreateTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, 400, "invalid body")
		return
	}
	if len(req.CategoryURLs) == 0 {
		respondError(w, 400, "category_urls required")
		return
	}
	if req.MaxPagesPerCategory <= 0 {
		req.MaxPagesPerCategory = 10
	}
	req.UserID = userID
	t, err := s.taskMgr.CreateTask(r.Context(), req)
	if err != nil {
		respondError(w, 500, err.Error())
		return
	}
	respond(w, 201, map[string]interface{}{"Status": true, "task_id": t.ID})
}

func (s *Server) listTasks(w http.ResponseWriter, r *http.Request) {
	userID, ok := getUserID(r)
	if !ok {
		respondError(w, 401, "unauthorized")
		return
	}
	tasks := s.taskMgr.ListTasks(userID)
	respond(w, 200, map[string]interface{}{"Status": true, "data": map[string]interface{}{"tasks": tasks, "total": len(tasks)}})
}

func (s *Server) getTask(w http.ResponseWriter, r *http.Request) {
	t, err := s.taskMgr.GetTaskFromCache(r.Context(), chi.URLParam(r, "taskID"))
	if err != nil {
		respondError(w, 404, err.Error())
		return
	}
	respond(w, 200, map[string]interface{}{"Status": true, "data": t})
}

func (s *Server) getTaskAds(w http.ResponseWriter, r *http.Request) {
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 50
	}
	ads, total, err := s.db.GetAdsByTask(r.Context(), chi.URLParam(r, "taskID"), offset, limit)
	if err != nil {
		respondError(w, 500, err.Error())
		return
	}
	respond(w, 200, map[string]interface{}{
		"Status": true,
		"data":   map[string]interface{}{"ads": ads, "total": total, "offset": offset, "limit": limit},
	})
}

func (s *Server) pauseTask(w http.ResponseWriter, r *http.Request) {
	if err := s.taskMgr.PauseTask(chi.URLParam(r, "taskID")); err != nil {
		respondError(w, 400, err.Error())
		return
	}
	respond(w, 200, map[string]interface{}{"Status": true, "message": "paused"})
}

func (s *Server) resumeTask(w http.ResponseWriter, r *http.Request) {
	if err := s.taskMgr.ResumeTask(chi.URLParam(r, "taskID")); err != nil {
		respondError(w, 400, err.Error())
		return
	}
	respond(w, 200, map[string]interface{}{"Status": true, "message": "resumed"})
}

func (s *Server) stopTask(w http.ResponseWriter, r *http.Request) {
	if err := s.taskMgr.StopTask(chi.URLParam(r, "taskID")); err != nil {
		respondError(w, 400, err.Error())
		return
	}
	respond(w, 200, map[string]interface{}{"Status": true, "message": "stopped"})
}

func (s *Server) deleteTask(w http.ResponseWriter, r *http.Request) {
	if err := s.taskMgr.DeleteTask(chi.URLParam(r, "taskID")); err != nil {
		respondError(w, 400, err.Error())
		return
	}
	respond(w, 200, map[string]interface{}{"Status": true, "message": "deleted"})
}

// ==================== ADS ====================

func (s *Server) getAd(w http.ResponseWriter, r *http.Request) {
	adID := chi.URLParam(r, "adID")
	fresh := r.URL.Query().Get("fresh") == "true"

	ad, err := s.db.GetAdWithImages(r.Context(), adID)
	if err != nil {
		respondError(w, 404, "ad not found")
		return
	}

	if fresh {
		if freshAd, err := s.taskMgr.InstantRecheck(r.Context(), adID); err == nil && freshAd != nil {
			ad = freshAd
			if ad.Images == nil {
				if full, err := s.db.GetAdWithImages(r.Context(), adID); err == nil {
					ad.Images = full.Images
				}
			}
		}
	} else if !ad.LastCheckedAt.IsZero() && time.Since(ad.LastCheckedAt) > 10*time.Minute {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			s.taskMgr.InstantRecheck(ctx, adID)
		}()
	}

	metrics, _ := s.db.GetAdMetrics(r.Context(), adID)
	s.cache.CacheAd(r.Context(), ad)
	respond(w, 200, map[string]interface{}{
		"Status": true,
		"data":   map[string]interface{}{"ad": ad, "metrics": metrics},
	})
}

func (s *Server) recheckAd(w http.ResponseWriter, r *http.Request) {
	adID := chi.URLParam(r, "adID")
	ad, err := s.taskMgr.InstantRecheck(r.Context(), adID)
	if err != nil {
		respondError(w, 500, err.Error())
		return
	}
	metrics, _ := s.db.GetAdMetrics(r.Context(), adID)
	respond(w, 200, map[string]interface{}{
		"Status": true,
		"data":   map[string]interface{}{"ad": ad, "metrics": metrics},
	})
}

func (s *Server) searchAdsAdvanced(w http.ResponseWriter, r *http.Request) {
	var req kl.AdSearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, 400, "invalid body")
		return
	}
	if req.PerPage <= 0 {
		req.PerPage = 50
	}
	if req.Page <= 0 {
		req.Page = 1
	}
	ads, total, err := s.db.SearchAdsWithMetrics(r.Context(), req)
	if err != nil {
		respondError(w, 500, err.Error())
		return
	}
	pages := total / req.PerPage
	if total%req.PerPage > 0 {
		pages++
	}
	respond(w, 200, map[string]interface{}{
		"Status": true,
		"data": map[string]interface{}{
			"ads": ads, "total": total, "page": req.Page, "per_page": req.PerPage, "pages": pages,
		},
	})
}

func (s *Server) getAdHistory(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 100
	}
	history, err := s.db.GetAdHistory(r.Context(), chi.URLParam(r, "adID"), limit)
	if err != nil {
		respondError(w, 500, err.Error())
		return
	}
	respond(w, 200, map[string]interface{}{"Status": true, "data": history})
}

func (s *Server) getAdStatistics(w http.ResponseWriter, r *http.Request) {
	adID := chi.URLParam(r, "adID")
	metrics, _ := s.db.GetAdMetrics(r.Context(), adID)
	chart, _ := s.db.GetAdChartData(r.Context(), adID)
	history, _ := s.db.GetAdHistory(r.Context(), adID, 1000)
	totalChanges := 0
	if history != nil {
		totalChanges = len(history)
	}
	respond(w, 200, map[string]interface{}{
		"Status": true,
		"data": kl.AdStatistics{
			AdID: adID, Metrics: metrics, Chart: chart, TotalChanges: totalChanges,
		},
	})
}

func (s *Server) recheckAll(w http.ResponseWriter, r *http.Request) {
	s.taskMgr.TriggerFullRecheck()
	respond(w, 200, map[string]interface{}{"Status": true, "message": "priority recheck triggered"})
}

// ==================== EXPORT ====================

func (s *Server) exportAds(w http.ResponseWriter, r *http.Request) {
	var req kl.AdSearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, 400, "invalid body")
		return
	}
	req.Page = 1
	req.PerPage = 5000

	ads, _, err := s.db.SearchAdsWithMetrics(r.Context(), req)
	if err != nil {
		respondError(w, 500, err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename=ads_export.csv")
	w.WriteHeader(200)

	cw := csv.NewWriter(w)
	cw.Write([]string{
		"id", "title", "price_eur", "views", "category_id", "location_id",
		"poster_type", "ad_status", "url", "start_date",
		"views_per_hour", "views_delta_1h", "views_delta_24h",
		"price_dropped", "price_change_pct", "first_seen_at",
	})

	for _, ad := range ads {
		vph, d1h, d24h := "0", "0", "0"
		dropped, changePct := "false", "0"
		if ad.Metrics != nil {
			vph = fmt.Sprintf("%.2f", ad.Metrics.ViewsPerHour)
			d1h = strconv.Itoa(ad.Metrics.ViewsDelta1h)
			d24h = strconv.Itoa(ad.Metrics.ViewsDelta24h)
			dropped = strconv.FormatBool(ad.Metrics.PriceDropped)
			changePct = fmt.Sprintf("%.2f", ad.Metrics.PriceChangePct)
		}
		cw.Write([]string{
			ad.ID, ad.Title, fmt.Sprintf("%.2f", ad.PriceEUR),
			strconv.Itoa(ad.Views), ad.CategoryID, ad.LocationID,
			ad.PosterType, ad.AdStatus, ad.URL, ad.StartDate,
			vph, d1h, d24h, dropped, changePct,
			ad.FirstSeenAt.Format(time.RFC3339),
		})
	}
	cw.Flush()
}

// ==================== BATCH ====================

func (s *Server) batchGetAds(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IDs []string `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.IDs) == 0 {
		respondError(w, 400, "ids required")
		return
	}
	if len(req.IDs) > 50 {
		req.IDs = req.IDs[:50]
	}

	ads, err := s.db.GetAdsBatch(r.Context(), req.IDs)
	if err != nil {
		respondError(w, 500, err.Error())
		return
	}
	respond(w, 200, map[string]interface{}{
		"Status": true,
		"data":   map[string]interface{}{"ads": ads, "total": len(ads)},
	})
}

// ==================== SELLERS ====================

func (s *Server) getSellerProfile(w http.ResponseWriter, r *http.Request) {
	sellerID := chi.URLParam(r, "sellerID")
	profile, err := s.db.GetSellerProfile(r.Context(), sellerID)
	if err != nil {
		respondError(w, 404, "seller not found")
		return
	}
	respond(w, 200, map[string]interface{}{"Status": true, "data": profile})
}

func (s *Server) getSellerAds(w http.ResponseWriter, r *http.Request) {
	sellerID := chi.URLParam(r, "sellerID")
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	ads, total, err := s.db.GetSellerAds(r.Context(), sellerID, offset, limit)
	if err != nil {
		respondError(w, 500, err.Error())
		return
	}
	respond(w, 200, map[string]interface{}{
		"Status": true,
		"data":   map[string]interface{}{"ads": ads, "total": total, "offset": offset, "limit": limit},
	})
}

// ==================== NOTIFICATIONS ====================

func (s *Server) getNotifications(w http.ResponseWriter, r *http.Request) {
	userID, ok := getUserID(r)
	if !ok {
		respondError(w, 401, "unauthorized")
		return
	}
	onlyUnread := r.URL.Query().Get("unread") == "true"
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 50
	}
	notifs, err := s.db.GetNotifications(r.Context(), userID, onlyUnread, limit)
	if err != nil {
		respondError(w, 500, err.Error())
		return
	}
	unreadCount, _ := s.db.GetUnreadNotificationCount(r.Context(), userID)
	respond(w, 200, map[string]interface{}{
		"Status": true,
		"data":   map[string]interface{}{"notifications": notifs, "unread_count": unreadCount},
	})
}

func (s *Server) markNotificationRead(w http.ResponseWriter, r *http.Request) {
	userID, ok := getUserID(r)
	if !ok {
		respondError(w, 401, "unauthorized")
		return
	}
	notifID, err := strconv.ParseInt(chi.URLParam(r, "notifID"), 10, 64)
	if err != nil {
		respondError(w, 400, "invalid notification id")
		return
	}
	if err := s.db.MarkNotificationRead(r.Context(), notifID, userID); err != nil {
		respondError(w, 500, err.Error())
		return
	}
	respond(w, 200, map[string]interface{}{"Status": true, "message": "marked as read"})
}

func (s *Server) markAllNotificationsRead(w http.ResponseWriter, r *http.Request) {
	userID, ok := getUserID(r)
	if !ok {
		respondError(w, 401, "unauthorized")
		return
	}
	if err := s.db.MarkAllNotificationsRead(r.Context(), userID); err != nil {
		respondError(w, 500, err.Error())
		return
	}
	respond(w, 200, map[string]interface{}{"Status": true, "message": "all marked as read"})
}

// ==================== CATEGORIES ====================

func (s *Server) getCategories(w http.ResponseWriter, r *http.Request) {
	cats, err := s.db.GetCategories(r.Context(), r.URL.Query().Get("parent_id"))
	if err != nil {
		respondError(w, 500, err.Error())
		return
	}
	respond(w, 200, map[string]interface{}{"Status": true, "data": cats})
}

func (s *Server) getCategoryTree(w http.ResponseWriter, r *http.Request) {
	cats, err := s.db.GetAllCategories(r.Context())
	if err != nil {
		respondError(w, 500, err.Error())
		return
	}
	respond(w, 200, map[string]interface{}{"Status": true, "data": cats})
}

func (s *Server) searchCategories(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	if q == "" {
		respondError(w, 400, "q required")
		return
	}
	cats, err := s.db.SearchCategories(r.Context(), q)
	if err != nil {
		respondError(w, 500, err.Error())
		return
	}
	respond(w, 200, map[string]interface{}{"Status": true, "data": cats})
}

func (s *Server) syncCategories(w http.ResponseWriter, r *http.Request) {
	go s.taskMgr.TriggerCategorySync()
	respond(w, 200, map[string]interface{}{"Status": true, "message": "sync triggered"})
}

// ==================== SAVED FILTERS ====================

func (s *Server) getSavedFilters(w http.ResponseWriter, r *http.Request) {
	userID, ok := getUserID(r)
	if !ok {
		respondError(w, 401, "unauthorized")
		return
	}
	filters, err := s.db.GetSavedFilters(r.Context(), userID)
	if err != nil {
		respondError(w, 500, err.Error())
		return
	}
	respond(w, 200, map[string]interface{}{"Status": true, "data": filters})
}

func (s *Server) createSavedFilter(w http.ResponseWriter, r *http.Request) {
	userID, ok := getUserID(r)
	if !ok {
		respondError(w, 401, "unauthorized")
		return
	}
	var sf kl.SavedFilter
	if err := json.NewDecoder(r.Body).Decode(&sf); err != nil {
		respondError(w, 400, "invalid body")
		return
	}
	sf.ID = kl.GenerateSessionID()
	sf.UserID = userID
	sf.CreatedAt = time.Now()
	sf.UpdatedAt = time.Now()
	if err := s.db.SaveFilter(r.Context(), &sf); err != nil {
		respondError(w, 500, err.Error())
		return
	}
	respond(w, 201, map[string]interface{}{"Status": true, "data": sf})
}

func (s *Server) deleteSavedFilter(w http.ResponseWriter, r *http.Request) {
	userID, ok := getUserID(r)
	if !ok {
		respondError(w, 401, "unauthorized")
		return
	}
	if err := s.db.DeleteSavedFilter(r.Context(), chi.URLParam(r, "filterID"), userID); err != nil {
		respondError(w, 500, err.Error())
		return
	}
	respond(w, 200, map[string]interface{}{"Status": true, "message": "deleted"})
}

// ==================== DASHBOARD ====================

func (s *Server) dashboard(w http.ResponseWriter, r *http.Request) {
	stats, err := s.db.GetDashboardStatsV2(r.Context())
	if err != nil {
		respondError(w, 500, err.Error())
		return
	}
	stats.ProxyCount = s.proxyPool.Count()
	stats.RunningTasks = len(s.taskMgr.ListTasks(""))
	respond(w, 200, map[string]interface{}{"Status": true, "data": stats, "counters": s.cache.GetStats(r.Context())})
}

func (s *Server) debugRawFetch(w http.ResponseWriter, r *http.Request) {
	adID := chi.URLParam(r, "adID")
	if adID == "" {
		respondError(w, 400, "adID required")
		return
	}
	result := s.taskMgr.Scraper().DebugRawFetch(r.Context(), adID)
	respond(w, 200, result)
}

func (s *Server) debugBatchCounters(w http.ResponseWriter, r *http.Request) {
	count := 50
	if c := r.URL.Query().Get("count"); c != "" {
		if n, err := strconv.Atoi(c); err == nil && n > 0 && n <= 500 {
			count = n
		}
	}
	ids, err := s.db.QueryAdIDs(r.Context(), `SELECT id FROM ads WHERE is_active = true AND is_deleted = false LIMIT $1`, count)
	if err != nil {
		respondError(w, 500, err.Error())
		return
	}
	result := s.taskMgr.Scraper().DebugBatchCounters(r.Context(), ids)
	respond(w, 200, result)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	count, _ := s.db.GetAdCount(r.Context())
	respond(w, 200, map[string]interface{}{"status": "ok", "ads": count, "proxies": s.proxyPool.Count()})
}

func (s *Server) proxyStats(w http.ResponseWriter, r *http.Request) {
	respond(w, 200, map[string]interface{}{
		"Status": true,
		"data":   map[string]interface{}{"count": s.proxyPool.Count(), "stats": s.proxyPool.Stats()},
	})
}

// ==================== HELPERS ====================

func respond(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func respondError(w http.ResponseWriter, status int, message string) {
	respond(w, status, map[string]interface{}{"Status": false, "message": message})
}
