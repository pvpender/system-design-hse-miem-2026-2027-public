// Package config читает конфигурацию сервиса из переменных окружения.
// Единственный источник настроек — env; значения по умолчанию подходят для docker compose.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config — все настройки процесса soldout.
type Config struct {
	HTTPAddr          string
	DatabaseURL       string // пул приложения (с шага A — через PgBouncer)
	DatabaseURLDirect string // прямое подключение к PostgreSQL: миграции и seed (сессионные advisory-lock golang-migrate)
	ValkeyAddr        string
	DBMaxConns        int32
	DBMinConns        int32
	DBMaxConnIdleTime time.Duration
	DBAcquireTimeout  time.Duration // ожидание свободного соединения → 503 db_busy
	HandlerTimeout    time.Duration // per-handler context.WithTimeout, меньше WriteTimeout сервера
	ExpirerBatch      int
	CacheInvalidate   bool          // шаг B: инвалидация кэша карты после commit подписчика (слом №1: false)
	CacheTTL          time.Duration // страховка от потерянного события
	EventbusWorkers   int
	EventbusBuffer    int
	HoldTTL           time.Duration
	ExpirerInterval   time.Duration
	AdmissionTTL      time.Duration
	LogLevel          string
	LogFormat         string
	ShutdownTimeout   time.Duration
	MigrateOnStart    bool
	SeedOnStart       bool
	SeedSeatsPerEvent int
	OTelEndpoint      string  // OTEL_EXPORTER_OTLP_ENDPOINT (gRPC); пусто — трейсы не экспортируются
	OTelSampleRatio   float64 // OTEL_TRACES_SAMPLER_ARG: доля корневых трейсов; на стенде 1.0, в проде 0.01–0.1
	OTelNoisyRatio    float64 // OTEL_NOISY_SAMPLER_ARG: доля для hold, входа в очередь и карты зала (шторм)
	Environment       string  // deployment.environment
	OutboxDelete      bool    // OUTBOX_DELETE_AFTER_INSERT: true — CDC (строка удаляется в той же транзакции), false — polling (lite)
}

// FromEnv собирает Config из окружения. Ошибка — только при непарсируемом значении.
func FromEnv() (Config, error) {
	var err error
	c := Config{
		HTTPAddr:     getenv("HTTP_ADDR", ":8080"),
		DatabaseURL:  getenv("DATABASE_URL", "postgres://soldout:soldout@localhost:5432/soldout?sslmode=disable"),
		ValkeyAddr:   getenv("VALKEY_ADDR", "localhost:6379"),
		LogLevel:     getenv("LOG_LEVEL", "info"),
		LogFormat:    getenv("LOG_FORMAT", "json"),
		OTelEndpoint: os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		Environment:  getenv("DEPLOYMENT_ENVIRONMENT", "lab"),
	}
	c.DatabaseURLDirect = getenv("DATABASE_URL_DIRECT", c.DatabaseURL)
	if c.DBMaxConns, err = int32Env("DB_MAX_CONNS", 200); err != nil {
		return c, err
	}
	if c.DBMinConns, err = int32Env("DB_MIN_CONNS", 2); err != nil {
		return c, err
	}
	if c.DBMaxConnIdleTime, err = durEnv("DB_MAX_CONN_IDLE", 5*time.Minute); err != nil {
		return c, err
	}
	if c.DBAcquireTimeout, err = durEnv("DB_ACQUIRE_TIMEOUT", 2*time.Second); err != nil {
		return c, err
	}
	if c.HandlerTimeout, err = durEnv("HANDLER_TIMEOUT", 3*time.Second); err != nil {
		return c, err
	}
	if c.ExpirerBatch, err = intEnv("HOLD_EXPIRER_BATCH", 500); err != nil {
		return c, err
	}
	if c.CacheInvalidate, err = boolEnv("CACHE_INVALIDATE", true); err != nil {
		return c, err
	}
	if c.CacheTTL, err = durEnv("CACHE_TTL", 30*time.Second); err != nil {
		return c, err
	}
	if c.EventbusWorkers, err = intEnv("EVENTBUS_WORKERS", 4); err != nil {
		return c, err
	}
	if c.EventbusBuffer, err = intEnv("EVENTBUS_BUFFER", 10000); err != nil {
		return c, err
	}
	if c.HoldTTL, err = durEnv("HOLD_TTL", 10*time.Minute); err != nil {
		return c, err
	}
	if c.ExpirerInterval, err = durEnv("HOLD_EXPIRER_INTERVAL", 5*time.Second); err != nil {
		return c, err
	}
	if c.AdmissionTTL, err = durEnv("ADMISSION_TTL", 15*time.Minute); err != nil {
		return c, err
	}
	if c.ShutdownTimeout, err = durEnv("SHUTDOWN_TIMEOUT", 10*time.Second); err != nil {
		return c, err
	}
	if c.MigrateOnStart, err = boolEnv("MIGRATE_ON_START", true); err != nil {
		return c, err
	}
	if c.SeedOnStart, err = boolEnv("SEED_ON_START", true); err != nil {
		return c, err
	}
	if c.SeedSeatsPerEvent, err = intEnv("SEED_SEATS_PER_EVENT", 40000); err != nil {
		return c, err
	}
	if c.OTelSampleRatio, err = floatEnv("OTEL_TRACES_SAMPLER_ARG", 1.0); err != nil {
		return c, err
	}
	if c.OutboxDelete, err = boolEnv("OUTBOX_DELETE_AFTER_INSERT", true); err != nil {
		return c, err
	}
	if c.OTelNoisyRatio, err = floatEnv("OTEL_NOISY_SAMPLER_ARG", 0.1); err != nil {
		return c, err
	}
	return c, nil
}

func getenv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func durEnv(key string, def time.Duration) (time.Duration, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q: %w", key, v, err)
	}
	return d, nil
}

func intEnv(key string, def int) (int, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q: %w", key, v, err)
	}
	return n, nil
}

func int32Env(key string, def int32) (int32, error) {
	n, err := intEnv(key, int(def))
	return int32(n), err
}

func floatEnv(key string, def float64) (float64, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q: %w", key, v, err)
	}
	return f, nil
}

func boolEnv(key string, def bool) (bool, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("config: %s=%q: %w", key, v, err)
	}
	return b, nil
}
