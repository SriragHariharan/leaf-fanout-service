package models

import "time"

// Post is one row in feed_posts (shared with feed-service-go).
type Post struct {
	PostID    string    `json:"post_id" gorm:"primaryKey"`
	MediaURL  string    `json:"media_url" gorm:"not null"`
	Content   string    `json:"content" gorm:"not null"`
	OwnerID   string    `json:"owner_id" gorm:"not null"`
	CreatedAt time.Time `json:"created_at" gorm:"not null"`
	UpdatedAt time.Time `json:"updated_at" gorm:"not null"`
}

func (Post) TableName() string { return "feed_posts" }
