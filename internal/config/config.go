package config

import (
	"os"
	"strconv"
)

// Config holds all configuration for the application.
type Config struct {
	// Server
	GRPCPort    string
	MetricsPort string

	// Database
	PostgresDSN string

	// Redis
	RedisAddr     string
	RedisPassword string
	RedisDB       int

	// Worker
	WorkerConcurrency int

	// Logging
	LogLevel string
}

// Load reads configuration from environment variables.
func Load() *Config {
	return &Config{
		GRPCPort:          getEnv("GRPC_PORT", "50051"),
		MetricsPort:       getEnv("METRICS_PORT", "9090"),
		PostgresDSN:       getEnv("POSTGRES_DSN", "postgres://postgres:postgres@localhost:5432/scheduler?sslmode=disable"),
		RedisAddr:         getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:     getEnv("REDIS_PASSWORD", ""),
		RedisDB:           getEnvInt("REDIS_DB", 0),
		WorkerConcurrency: getEnvInt("WORKER_CONCURRENCY", 5),
		LogLevel:          getEnv("LOG_LEVEL", "info"),
	}
}

// getEnv returns the value of an environment variable or a default value.
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

// getEnvInt returns the integer value of an environment variable or a default value.
func getEnvInt(key string, defaultValue int) int {
	if value, exists := os.LookupEnv(key); exists {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}
