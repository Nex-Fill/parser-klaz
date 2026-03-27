package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/danamakarenko/klaz-parser/internal/admin"
	"github.com/danamakarenko/klaz-parser/internal/api"
	"github.com/danamakarenko/klaz-parser/internal/config"
	"github.com/danamakarenko/klaz-parser/internal/media"
	"github.com/danamakarenko/klaz-parser/internal/parser"
	"github.com/danamakarenko/klaz-parser/internal/proxy"
	"github.com/danamakarenko/klaz-parser/internal/storage"
	"github.com/danamakarenko/klaz-parser/internal/task"
)

func main() {
	logBuf := admin.NewLogBuffer(1000)
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "15:04:05"}
	multi := zerolog.MultiLevelWriter(consoleWriter, logBuf)
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = zerolog.New(multi).With().Timestamp().Logger()

	godotenv.Load()
	cfg := config.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Info().Str("host", cfg.DB.Host).Msg("connecting to postgres")
	db, err := storage.NewPostgres(ctx, cfg.DB.DSN(), cfg.DB.MaxConns, cfg.DB.MinConns)
	if err != nil {
		log.Fatal().Err(err).Msg("postgres connect failed")
	}
	defer db.Close()

	log.Info().Str("addr", cfg.Redis.Addr()).Msg("connecting to redis")
	cache, err := storage.NewCache(cfg.Redis.Addr(), cfg.Redis.Password, cfg.Redis.DB, cfg.Redis.CacheTTL)
	if err != nil {
		log.Fatal().Err(err).Msg("redis connect failed")
	}
	defer cache.Close()

	liveConfig := config.NewLiveConfig(cache.Client(), db.Pool(), config.DefaultLiveSettings(cfg))
	liveConfig.Load(ctx)
	liveConfig.StartWatcher(ctx)
	log.Info().Msg("live config loaded")

	proxyPool := proxy.NewPool(
		liveConfig.Get().ProxySourceURL, cfg.Proxy.FilePath,
		liveConfig.Get().ProxyUpdateInterval, cfg.Proxy.HealthCheck,
		cfg.Proxy.MinPoolSize,
	)
	if err := proxyPool.Start(ctx); err != nil {
		log.Warn().Err(err).Msg("proxy pool start warning")
	}
	defer proxyPool.Stop()

	var mediaPipeline *media.Pipeline
	if cfg.S3.AccessKey != "" {
		mediaPipeline, err = media.NewPipeline(cfg.S3, cfg.Workers.ImageWorkers)
		if err != nil {
			log.Warn().Err(err).Msg("media pipeline init failed, images disabled")
		} else {
			defer mediaPipeline.Close()
		}
	}

	scraper, err := parser.NewScraper(cfg.Parser, liveConfig, proxyPool, db, cache, mediaPipeline, cfg.Workers.ParseWorkers)
	if err != nil {
		log.Fatal().Err(err).Msg("scraper init failed")
	}
	defer scraper.Close()

	taskMgr := task.NewManager(db, cache, scraper)
	taskMgr.StartPriorityRecheckLoop(ctx)
	taskMgr.StartMetricsRefreshLoop(ctx)
	taskMgr.StartCategorySyncLoop(ctx)

	adminHandler := admin.NewHandler(liveConfig, taskMgr, db, cache, proxyPool, logBuf,
		getEnvDefault("ADMIN_USER", "admin"), getEnvDefault("ADMIN_PASSWORD", "admin"))

	srv := api.NewServer(taskMgr, db, cache, proxyPool, cfg.Server.JWTSecret)
	srv.StartWSHub(ctx)

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)

	mux := http.NewServeMux()
	mux.Handle("/admin/", http.StripPrefix("/admin", adminHandler.Routes()))
	mux.Handle("/", srv.Router())

	httpServer := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Info().Str("addr", addr).Msg("server starting")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("server error")
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info().Msg("shutting down...")
	scraper.Close()
	taskMgr.Shutdown()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("server shutdown error")
	}
	cancel()
	log.Info().Msg("server stopped")
}

func getEnvDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
