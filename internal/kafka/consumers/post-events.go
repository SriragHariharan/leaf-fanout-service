package consumers

import (
	"context"
	"encoding/json"
	"log"
	"strings"

	kgo "github.com/segmentio/kafka-go"
	"github.com/sriraghariharan/leaf-fanout-service/internal/kafka"
	"github.com/sriraghariharan/leaf-fanout-service/internal/kafka/events"
	"github.com/sriraghariharan/leaf-fanout-service/internal/service"
)

const PostEventsTopic = "post.events"

func NewPostEventsReader(consumerGroupID string) *kgo.Reader {
	return kgo.NewReader(kgo.ReaderConfig{
		Brokers: kafka.Brokers,
		Topic:   PostEventsTopic,
		GroupID: consumerGroupID,
		Dialer:  kafka.Dialer,
	})
}

func RunPostEventsConsumer(ctx context.Context, consumerGroupID string, svc service.IFanoutService) error {
	r := NewPostEventsReader(consumerGroupID)
	defer func() { _ = r.Close() }()

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			return err
		}

		var event events.PostEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Printf("kafka post.events: invalid json partition=%d offset=%d: %v", m.Partition, m.Offset, err)
			continue
		}

		switch strings.TrimSpace(event.EventType) {
		case events.EventPostCreated:
			if err := handlePostCreated(ctx, svc, event, m.Partition, m.Offset); err != nil {
				log.Printf("kafka post.events: created failed: %v", err)
			}
		case events.EventPostEdited:
			if err := handlePostEdited(ctx, svc, event, m.Partition, m.Offset); err != nil {
				log.Printf("kafka post.events: edited failed: %v", err)
			}
		case events.EventPostDeleted:
			if err := handlePostDeleted(ctx, svc, event, m.Partition, m.Offset); err != nil {
				log.Printf("kafka post.events: deleted failed: %v", err)
			}
		default:
			log.Printf("kafka post.events: unknown eventType=%q partition=%d offset=%d", event.EventType, m.Partition, m.Offset)
		}
	}
}

func handlePostCreated(ctx context.Context, svc service.IFanoutService, event events.PostEvent, partition int, offset int64) error {
	postID := strings.TrimSpace(event.PostID)
	ownerID := strings.TrimSpace(event.OwnerID)
	content := strings.TrimSpace(event.Content)
	if postID == "" || ownerID == "" || content == "" {
		log.Printf("kafka post.events: created missing fields partition=%d offset=%d", partition, offset)
		return nil
	}

	mediaURL := ""
	if event.ImageURL != nil {
		mediaURL = strings.TrimSpace(*event.ImageURL)
	}

	err := svc.ProcessPostCreated(ctx, service.PostCreatedInput{
		PostID:   postID,
		Content:  content,
		MediaURL: mediaURL,
		OwnerID:  ownerID,
	})
	if err != nil {
		return err
	}

	log.Printf("kafka post.events: created ok postID=%s partition=%d offset=%d", postID, partition, offset)
	return nil
}

func handlePostEdited(ctx context.Context, svc service.IFanoutService, event events.PostEvent, partition int, offset int64) error {
	postID := strings.TrimSpace(event.PostID)
	ownerID := strings.TrimSpace(event.OwnerID)
	content := strings.TrimSpace(event.Content)
	if postID == "" || ownerID == "" || content == "" {
		log.Printf("kafka post.events: edited missing fields partition=%d offset=%d", partition, offset)
		return nil
	}

	mediaURL := ""
	if event.ImageURL != nil {
		mediaURL = strings.TrimSpace(*event.ImageURL)
	}

	err := svc.ProcessPostEdited(ctx, service.PostEditedInput{
		PostID:   postID,
		Content:  content,
		MediaURL: mediaURL,
		OwnerID:  ownerID,
	})
	if err != nil {
		return err
	}

	log.Printf("kafka post.events: edited ok postID=%s partition=%d offset=%d", postID, partition, offset)
	return nil
}

func handlePostDeleted(ctx context.Context, svc service.IFanoutService, event events.PostEvent, partition int, offset int64) error {
	postID := strings.TrimSpace(event.PostID)
	if postID == "" {
		log.Printf("kafka post.events: deleted missing postID partition=%d offset=%d", partition, offset)
		return nil
	}

	err := svc.ProcessPostDeleted(ctx, postID)
	if err != nil {
		return err
	}

	log.Printf("kafka post.events: deleted ok postID=%s partition=%d offset=%d", postID, partition, offset)
	return nil
}
