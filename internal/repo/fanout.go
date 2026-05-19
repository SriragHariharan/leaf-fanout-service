package repo

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/sriraghariharan/leaf-fanout-service/internal/models"
	"gorm.io/gorm"
)

const (
	queryChunkSize  = 1000
	insertBatchSize = 1000
)

type FanoutRepo struct {
	db *gorm.DB
}

func NewFanoutRepo(db *gorm.DB) *FanoutRepo {
	return &FanoutRepo{db: db}
}

// WriteTimelines inserts feed_feeds rows for each recipient (viewer) who does not already have this post.
func (r *FanoutRepo) WriteTimelines(ctx context.Context, authorID, postID string, friendIDs []string) error {
	if len(friendIDs) == 0 {
		return nil
	}

	existing := make(map[string]struct{})
	for i := 0; i < len(friendIDs); i += queryChunkSize {
		end := i + queryChunkSize
		if end > len(friendIDs) {
			end = len(friendIDs)
		}
		chunk := friendIDs[i:end]

		var userIDs []string
		err := r.db.WithContext(ctx).
			Model(&models.Feed{}).
			Where("post_id = ? AND user_id IN ?", postID, chunk).
			Pluck("user_id", &userIDs).Error
		if err != nil {
			return err
		}
		for _, id := range userIDs {
			existing[id] = struct{}{}
		}
	}

	now := time.Now().UTC()
	batch := make([]models.Feed, 0, len(friendIDs))
	for _, friendID := range friendIDs {
		if _, ok := existing[friendID]; ok {
			continue
		}
		batch = append(batch, models.Feed{
			FeedID:      uuid.NewString(),
			UserID:      friendID,
			AuthorID:    authorID,
			PostID:      postID,
			IsLiked:     false,
			IsCommented: false,
			CreatedAt:   now,
			UpdatedAt:   now,
		})
	}

	if len(batch) == 0 {
		return nil
	}

	return r.db.WithContext(ctx).CreateInBatches(batch, insertBatchSize).Error
}

func (r *FanoutRepo) DeleteFeedsByPostID(ctx context.Context, postID string) error {
	return r.db.WithContext(ctx).
		Where("post_id = ?", postID).
		Delete(&models.Feed{}).Error
}
