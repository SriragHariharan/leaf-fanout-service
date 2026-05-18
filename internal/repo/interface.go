package repo

import (
	"context"

	"github.com/sriraghariharan/leaf-fanout-service/internal/models"
)

type IPostRepository interface {
	SavePost(ctx context.Context, post models.Post) error
	UpdatePost(ctx context.Context, postID, content, mediaURL string) error
	DeletePost(ctx context.Context, postID string) error
}

type IFanoutRepository interface {
	WriteTimelines(ctx context.Context, authorID, postID string, friendIDs []string) error
	DeleteFeedsByPostID(ctx context.Context, postID string) error
}