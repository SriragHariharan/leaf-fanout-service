package service

import "context"

type IFanoutService interface {
	ProcessPostCreated(ctx context.Context, in PostCreatedInput) error
}
