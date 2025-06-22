package models

import (
	"time"
)

type InviteLink struct {
	ID         uint   `gorm:"primaryKey"`
	DocumentID uint   `gorm:"not null;index"`
	Token      string `gorm:"uniqueIndex;not null"`
	Role       string `gorm:"type:varchar(20);not null"`
	Used       bool   `gorm:"not null;default:false"`
	ExpiresAt  *time.Time
	CreatedAt  time.Time

	Document Document `gorm:"foreignKey:DocumentID"`
}
