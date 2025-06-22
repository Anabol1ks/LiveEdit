package db

import (
	"fmt"

	"github.com/Anabol1ks/LiveEdit/internal/config"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var DB *gorm.DB

func ConnectDB(cfg *config.Config, log *zap.Logger) {
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s prefer_simple_protocol=true", cfg.DB.Host, cfg.DB.Port, cfg.DB.User, cfg.DB.Password, cfg.DB.Name, cfg.DB.SSLMode)

	var err error
	DB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{PrepareStmt: false})
	if err != nil {
		log.Fatal("Не удалось подключиться к базе данных: ", zap.Error(err))
	}

	log.Info("Успешное подключение к базе данных PostgreSQL")
}
