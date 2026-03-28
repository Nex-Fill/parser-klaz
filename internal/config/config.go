package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Server   ServerConfig
	DB       DBConfig
	Redis    RedisConfig
	S3       S3Config
	Parser   ParserConfig
	Proxy    ProxyConfig
	Workers  WorkerConfig
}

type ServerConfig struct {
	Host string
	Port int
	JWTSecret string
}

type DBConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Name     string
	SSLMode  string
	MaxConns int
	MinConns int
}

func (c DBConfig) DSN() string {
	return "postgres://" + c.User + ":" + c.Password + "@" + c.Host + ":" + strconv.Itoa(c.Port) + "/" + c.Name + "?sslmode=" + c.SSLMode
}

type RedisConfig struct {
	Host     string
	Port     int
	Password string
	DB       int
	CacheTTL time.Duration
}

func (c RedisConfig) Addr() string {
	return c.Host + ":" + strconv.Itoa(c.Port)
}

type S3Config struct {
	AccessKey       string
	SecretKey       string
	Endpoint        string
	Region          string
	BucketOriginals string
	BucketPreviews  string
	CDNBase         string
	PreviewWidth    int
}

type ParserConfig struct {
	KlazVersion     string
	BasicAuth       string
	BmpAPIKey       string
	BmpAPIURL       string
	RequestTimeout  time.Duration
	BatchSize       int
	PageSize        int
	MaxPagesPerCat  int
	RecheckInterval time.Duration
}

type ProxyConfig struct {
	FileURL       string
	FilePath      string
	UpdateInterval time.Duration
	HealthCheck    time.Duration
	MinPoolSize    int
}

type WorkerConfig struct {
	ParseWorkers   int
	ImageWorkers   int
	CheckWorkers   int
	MaxConcurrent  int
}

func Load() *Config {
	return &Config{
		Server: ServerConfig{
			Host:      getEnv("SERVER_HOST", "0.0.0.0"),
			Port:      getEnvInt("SERVER_PORT", 8080),
			JWTSecret: getEnv("JWT_SECRET", "change-me-in-production"),
		},
		DB: DBConfig{
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     getEnvInt("DB_PORT", 5432),
			User:     getEnv("DB_USER", "klaz"),
			Password: getEnv("DB_PASSWORD", "klaz"),
			Name:     getEnv("DB_NAME", "klaz"),
			SSLMode:  getEnv("DB_SSLMODE", "disable"),
			MaxConns: getEnvInt("DB_MAX_CONNS", 50),
			MinConns: getEnvInt("DB_MIN_CONNS", 10),
		},
		Redis: RedisConfig{
			Host:     getEnv("REDIS_HOST", "localhost"),
			Port:     getEnvInt("REDIS_PORT", 6379),
			Password: getEnv("REDIS_PASSWORD", ""),
			DB:       getEnvInt("REDIS_DB", 0),
			CacheTTL: getEnvDuration("REDIS_CACHE_TTL", 5*time.Minute),
		},
		S3: S3Config{
			AccessKey:       getEnv("S3_ACCESS_KEY", ""),
			SecretKey:       getEnv("S3_SECRET_KEY", ""),
			Endpoint:        getEnv("S3_ENDPOINT", "s3.eu-central-1.wasabisys.com"),
			Region:          getEnv("S3_REGION", "eu-central-1"),
			BucketOriginals: getEnv("S3_BUCKET_ORIGINALS", "klaz-photos-originals"),
			BucketPreviews:  getEnv("S3_BUCKET_PREVIEWS", "klaz-photos-preview"),
			CDNBase:         getEnv("S3_CDN_BASE", ""),
			PreviewWidth:    getEnvInt("S3_PREVIEW_WIDTH", 800),
		},
		Parser: ParserConfig{
			KlazVersion:     getEnv("KLAZ_VERSION", "100.53.1"),
			BasicAuth:       getEnv("KLAZ_BASIC_AUTH", "Basic aXBob25lOmc0Wmk5cTEw"),
			BmpAPIKey:       getEnv("BMP_API_KEY", ""),
			BmpAPIURL:       getEnv("BMP_API_URL", "https://sync.ez-captcha.com/createSyncTask"),
			RequestTimeout:  getEnvDuration("REQUEST_TIMEOUT", 30*time.Second),
			BatchSize:       getEnvInt("BATCH_SIZE", 20),
			PageSize:        getEnvInt("PAGE_SIZE", 100),
			MaxPagesPerCat:  getEnvInt("MAX_PAGES_PER_CAT", 50),
			RecheckInterval: getEnvDuration("RECHECK_INTERVAL", 1*time.Hour),
		},
		Proxy: ProxyConfig{
			FileURL:        getEnv("PROXY_URL", "https://sh.govno.de/proxies/text?country=Germany&maxTimeout=2000"),
			FilePath:       getEnv("PROXY_FILE", ""),
			UpdateInterval: getEnvDuration("PROXY_UPDATE_INTERVAL", 5*time.Minute),
			HealthCheck:    getEnvDuration("PROXY_HEALTH_CHECK", 30*time.Second),
			MinPoolSize:    getEnvInt("PROXY_MIN_POOL", 10),
		},
		Workers: WorkerConfig{
			ParseWorkers:  getEnvInt("PARSE_WORKERS", 250),
			ImageWorkers:  getEnvInt("IMAGE_WORKERS", 50),
			CheckWorkers:  getEnvInt("CHECK_WORKERS", 30),
			MaxConcurrent: getEnvInt("MAX_CONCURRENT", 300),
		},
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
