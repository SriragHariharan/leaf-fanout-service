package models

import "time"

// Feed is one row in feed_feeds (viewer UserID, post PostID, author AuthorID).
type Feed struct {
	FeedID      string    `json:"feed_id" gorm:"primaryKey;index:idx_feed_user_created_feed,priority:3"`
	UserID      string    `json:"user_id" gorm:"not null;index:idx_feed_user_created_feed,priority:1"`
	AuthorID    string    `json:"author_id" gorm:"not null;index:idx_feed_author"`
	PostID      string    `json:"post_id" gorm:"not null;index:idx_feed_post"`
	IsLiked     bool      `json:"is_liked" gorm:"default:false"`
	IsCommented bool      `json:"is_commented" gorm:"default:false"`
	CreatedAt   time.Time `json:"created_at" gorm:"not null;index:idx_feed_user_created_feed,priority:2"`
	UpdatedAt   time.Time `json:"updated_at" gorm:"not null"`
}

func (Feed) TableName() string { return "feed_feeds" }
