package consumers

import (
	"context"
	"errors"
	"log"

	"github.com/sriraghariharan/leaf-fanout-service/internal/service"
)

func RunConsumers(ctx context.Context, consumerGroupID string, svc service.IFanoutService) error {
	go func() {
		if err := RunPostEventsConsumer(ctx, consumerGroupID, svc); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("post.events consumer: %v", err)
		}
	}()

	return nil
}
