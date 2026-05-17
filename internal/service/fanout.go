package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sriraghariharan/leaf-fanout-service/internal/clients/friends"
	"github.com/sriraghariharan/leaf-fanout-service/internal/models"
	"github.com/sriraghariharan/leaf-fanout-service/internal/repo"
)

type FanoutService struct {
	postRepo   repo.IPostRepository
	fanoutRepo repo.IFanoutRepository
}

func NewService(postRepo repo.IPostRepository, fanoutRepo repo.IFanoutRepository) *FanoutService {
	return &FanoutService{postRepo: postRepo, fanoutRepo: fanoutRepo}
}

func (s *FanoutService) ProcessPostCreated(ctx context.Context, in PostCreatedInput) error {
	postID := strings.TrimSpace(in.PostID)
	ownerID := strings.TrimSpace(in.OwnerID)
	content := strings.TrimSpace(in.Content)
	if postID == "" || ownerID == "" || content == "" {
		return fmt.Errorf("postID, ownerID, and content are required")
	}

	now := time.Now().UTC()
	post := models.Post{
		PostID:    postID,
		MediaURL:  in.MediaURL,
		Content:   content,
		OwnerID:   ownerID,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := s.postRepo.SavePost(ctx, post); err != nil {
		return fmt.Errorf("save post: %w", err)
	}

	if err := s.fanoutToFriends(ctx, postID, ownerID); err != nil {
		return fmt.Errorf("fanout: %w", err)
	}
	return nil
}

func (s *FanoutService) fanoutToFriends(ctx context.Context, postID, authorID string) error {
	friendIDs, err := friends.FetchTopFriendIDs(ctx, authorID)
	if err != nil {
		return fmt.Errorf("fetch friend ids: %w", err)
	}

	if err := s.fanoutRepo.WriteTimelines(ctx, authorID, postID, friendIDs); err != nil {
		return fmt.Errorf("write timelines: %w", err)
	}
	return nil
}
