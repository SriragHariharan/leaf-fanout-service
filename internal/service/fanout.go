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

func (s *FanoutService) ProcessPostEdited(ctx context.Context, in PostEditedInput) error {
	postID := strings.TrimSpace(in.PostID)
	ownerID := strings.TrimSpace(in.OwnerID)
	content := strings.TrimSpace(in.Content)
	if postID == "" || ownerID == "" || content == "" {
		return fmt.Errorf("postID, ownerID, and content are required")
	}

	if err := s.postRepo.UpdatePost(ctx, postID, content, in.MediaURL); err != nil {
		return fmt.Errorf("update post: %w", err)
	}
	return nil
}

func (s *FanoutService) ProcessPostDeleted(ctx context.Context, postID string) error {
	postID = strings.TrimSpace(postID)
	if postID == "" {
		return fmt.Errorf("postID is required")
	}

	if err := s.fanoutRepo.DeleteFeedsByPostID(ctx, postID); err != nil {
		return fmt.Errorf("delete feeds: %w", err)
	}
	if err := s.postRepo.DeletePost(ctx, postID); err != nil {
		return fmt.Errorf("delete post: %w", err)
	}
	return nil
}

func (s *FanoutService) fanoutToFriends(ctx context.Context, postID, authorID string) error {
	friendIDs, err := friends.FetchTopFriendIDs(ctx, authorID)
	if err != nil {
		return fmt.Errorf("fetch friend ids: %w", err)
	}

	recipientIDs := append([]string{authorID}, friendIDs...)
	if err := s.fanoutRepo.WriteTimelines(ctx, authorID, postID, recipientIDs); err != nil {
		return fmt.Errorf("write timelines: %w", err)
	}
	return nil
}
