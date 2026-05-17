package repo

import (
	"context"

	"github.com/sriraghariharan/leaf-fanout-service/internal/models"
)

type IPostRepository interface {
	SavePost(ctx context.Context, post models.Post) error
}

type IFanoutRepository interface {
	WriteTimelines(ctx context.Context, authorID, postID string, friendIDs []string) error
}