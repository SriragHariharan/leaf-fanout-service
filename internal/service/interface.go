package service

import "context"

type IFanoutService interface {
	ProcessPostCreated(ctx context.Context, in PostCreatedInput) error
	ProcessPostEdited(ctx context.Context, in PostEditedInput) error
	ProcessPostDeleted(ctx context.Context, postID string) error
}
