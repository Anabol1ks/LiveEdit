package models

import "time"

type DocumentAccess struct {
	ID         uint   `gorm:"primaryKey"`
	DocumentID uint   `gorm:"not null"`
	UserID     uint   `gorm:"not null"`
	Role       string `gorm:"type:varchar(20);not null"` // editor, viewer
	CreatedAt  time.Time

	Document Document `gorm:"foreignKey:DocumentID"`
	User     User     `gorm:"foreignKey:UserID"`

	// Уникальный индекс, чтобы один пользователь не мог быть дважды в одном документе
	// Это будет миграцией добавлено: `UNIQUE(document_id, user_id)`
}
