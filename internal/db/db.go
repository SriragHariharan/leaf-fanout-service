package db

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// DB is the application-wide GORM handle after a successful Connect.
var DB *gorm.DB

const (
	defaultPingTimeout      = 15 * time.Second
	defaultMaxOpenConns     = 25
	defaultMaxIdleConns     = 15
	defaultConnMaxLifetime  = 30 * time.Minute
	defaultConnMaxIdleTime  = 5 * time.Minute
	envDatabaseURL          = "DATABASE_URL"
	envDBMaxOpenConns       = "DB_MAX_OPEN_CONNS"
	envDBMaxIdleConns       = "DB_MAX_IDLE_CONNS"
	envDBConnMaxLifetime    = "DB_CONN_MAX_LIFETIME"
	envDBConnMaxIdleTime    = "DB_CONN_MAX_IDLE_TIME"
	envGORMLogLevel         = "GORM_LOG_LEVEL" // silent | error | warn | info (default error)
)

// Connect opens a CockroachDB connection (PostgreSQL protocol), configures the pool,
// verifies connectivity with Ping, and assigns the global DB on success.
func Connect(ctx context.Context) (*gorm.DB, error) {
	dsn := strings.TrimSpace(os.Getenv(envDatabaseURL))
	if dsn == "" {
		return nil, fmt.Errorf("%s must be set to a CockroachDB / Postgres connection URL", envDatabaseURL)
	}

	gormLog, err := gormLogLevel()
	if err != nil {
		return nil, err
	}

	gdb, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(gormLog),
		// SkipDefaultTransaction can be enabled later per workload; keep defaults for safety.
	})
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	sqlDB, err := gdb.DB()
	if err != nil {
		return nil, fmt.Errorf("sql db from gorm: %w", err)
	}

	pool := poolConfigFromEnv()
	sqlDB.SetMaxOpenConns(pool.maxOpen)
	sqlDB.SetMaxIdleConns(pool.maxIdle)
	sqlDB.SetConnMaxLifetime(pool.maxLifetime)
	sqlDB.SetConnMaxIdleTime(pool.maxIdleTime)

	pingCtx := ctx
	if pingCtx == nil {
		pingCtx = context.Background()
	}
	pingCtx, cancel := context.WithTimeout(pingCtx, defaultPingTimeout)
	defer cancel()

	if err := sqlDB.PingContext(pingCtx); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping database (check %s, TLS, and network): %w", envDatabaseURL, err)
	}

	log.Printf("database: connected (CockroachDB/Postgres, pool max_open=%d max_idle=%d conn_max_lifetime=%s)",
		pool.maxOpen, pool.maxIdle, pool.maxLifetime)

	DB = gdb
	return gdb, nil
}

type poolConfig struct {
	maxOpen      int
	maxIdle      int
	maxLifetime  time.Duration
	maxIdleTime  time.Duration
}

func poolConfigFromEnv() poolConfig {
	c := poolConfig{
		maxOpen:     defaultMaxOpenConns,
		maxIdle:     defaultMaxIdleConns,
		maxLifetime: defaultConnMaxLifetime,
		maxIdleTime: defaultConnMaxIdleTime,
	}
	if v := os.Getenv(envDBMaxOpenConns); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.maxOpen = n
		}
	}
	if v := os.Getenv(envDBMaxIdleConns); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			c.maxIdle = n
		}
	}
	if v := os.Getenv(envDBConnMaxLifetime); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			c.maxLifetime = d
		}
	}
	if v := os.Getenv(envDBConnMaxIdleTime); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			c.maxIdleTime = d
		}
	}
	if c.maxIdle > c.maxOpen {
		c.maxIdle = c.maxOpen
	}
	return c
}

func gormLogLevel() (logger.LogLevel, error) {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(envGORMLogLevel))) {
	case "", "error":
		return logger.Error, nil
	case "silent":
		return logger.Silent, nil
	case "warn":
		return logger.Warn, nil
	case "info":
		return logger.Info, nil
	default:
		return 0, fmt.Errorf("%s must be one of silent, error, warn, info", envGORMLogLevel)
	}
}

// Close releases the global connection pool. Safe to call once at shutdown.
func Close() error {
	if DB == nil {
		return nil
	}
	sqlDB, err := DB.DB()
	if err != nil {
		return fmt.Errorf("sql db from gorm: %w", err)
	}
	DB = nil
	if err := sqlDB.Close(); err != nil {
		return fmt.Errorf("close database: %w", err)
	}
	return nil
}

// Health checks the database with PingContext. Use for readiness probes.
func Health(ctx context.Context) error {
	if DB == nil {
		return errors.New("database not initialized")
	}
	sqlDB, err := DB.DB()
	if err != nil {
		return fmt.Errorf("sql db from gorm: %w", err)
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		return fmt.Errorf("ping: %w", err)
	}
	return nil
}
