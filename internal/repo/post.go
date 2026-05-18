package repo

import (
	"context"
	"time"

	"github.com/sriraghariharan/leaf-fanout-service/internal/models"
	"gorm.io/gorm"
)

type PostRepo struct {
	db *gorm.DB
}

func NewPostRepo(db *gorm.DB) *PostRepo {
	return &PostRepo{db: db}
}

// SavePost inserts the post into feed_posts. Skips insert if post_id already exists (Kafka retries).
func (r *PostRepo) SavePost(ctx context.Context, post models.Post) error {
	var count int64
	err := r.db.WithContext(ctx).
		Model(&models.Post{}).
		Where("post_id = ?", post.PostID).
		Count(&count).Error
	if err != nil {
		return err
	}
	if count > 0 {
		return nil
	}
	return r.db.WithContext(ctx).Create(&post).Error
}

func (r *PostRepo) UpdatePost(ctx context.Context, postID, content, mediaURL string) error {
	return r.db.WithContext(ctx).
		Model(&models.Post{}).
		Where("post_id = ?", postID).
		Updates(map[string]interface{}{
			"content":    content,
			"media_url":  mediaURL,
			"updated_at": time.Now().UTC(),
		}).Error
}

func (r *PostRepo) DeletePost(ctx context.Context, postID string) error {
	return r.db.WithContext(ctx).
		Where("post_id = ?", postID).
		Delete(&models.Post{}).Error
}
