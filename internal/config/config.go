package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	AppPort       string
	AppEnv        string
	DB            DBConfig
	AccessSecret  string
	RefreshSecret string
}

type DBConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	Name     string
	SSLMode  string
}

func Load() *Config {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using system env")
	}

	return &Config{
		AppPort: getEnv("APP_PORT", "50051"),
		AppEnv:  getEnv("APP_ENV", "development"),
		DB: DBConfig{
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     getEnv("DB_PORT", "5432"),
			User:     getEnv("DB_USER", "postgres"),
			Password: getEnv("DB_PASSWORD", "postgres"),
			Name:     getEnv("DB_NAME", "liveedit"),
			SSLMode:  getEnv("DB_SSLMODE", "require"),
		},
		AccessSecret:  getEnv("ACCESS_SECRET", "access-secret"),
		RefreshSecret: getEnv("REFRESH_SECRET", "refresh-secret"),
	}
}

func getEnv(key, fallback string) string {
	if val, exists := os.LookupEnv(key); exists {
		return val
	}
	return fallback
}
