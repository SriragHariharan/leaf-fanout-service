package consumers

import (
	"context"
	"errors"
	"log"

	"github.com/sriraghariharan/leaf-fanout-service/internal/service"
)

func RunConsumers(ctx context.Context, consumerGroupID string, svc service.IFanoutService) error {
	go func() {
		if err := RunPostCreatedConsumer(ctx, consumerGroupID, svc); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("post created consumer: %v", err)
		}
	}()

	go func() {
		if err := RunPostUpdatedConsumer(ctx, consumerGroupID); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("post updated consumer: %v", err)
		}
	}()

	go func() {
		if err := RunPostDeletedConsumer(ctx, consumerGroupID); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("post deleted consumer: %v", err)
		}
	}()

	return nil
}