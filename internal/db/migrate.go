package db

import (
	"github.com/Anabol1ks/LiveEdit/internal/models"
	"go.uber.org/zap"
)

func Migrate(log *zap.Logger) {
	if err := DB.AutoMigrate(
		&models.User{},
		&models.Document{},
		&models.DocumentAccess{},
		&models.InviteLink{},
	); err != nil {
		log.Fatal("Ошибка при миграции таблиц: ", zap.Error(err))
	}

	log.Info("Автомиграция таблиц завершена успешно")
}
