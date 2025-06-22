package models

import "time"

type Document struct {
	ID        uint   `gorm:"primaryKey"`
	Title     string `gorm:"not null"`
	Content   string `gorm:"type:text"` // изначальное содержимое
	OwnerID   uint   `gorm:"not null"`
	Owner     User   `gorm:"foreignKey:OwnerID"`
	CreatedAt time.Time
	UpdatedAt time.Time
}
