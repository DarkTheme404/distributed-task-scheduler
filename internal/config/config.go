package config

import (
	"os"
	"strconv"
)

type Config struct {
	GRPCPort    string
	MetricsPort string

	PostgresDSN string

	RedisAddr     string
	RedisPassword string
	RedisDB       int

	WorkerConcurrency int

	LogLevel string
}

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

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value, exists := os.LookupEnv(key); exists {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}
