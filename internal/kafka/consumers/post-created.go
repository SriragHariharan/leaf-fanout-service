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

const PostCreatedTopic = "post.created"

func NewPostCreatedReader(consumerGroupID string) *kgo.Reader {
	return kgo.NewReader(kgo.ReaderConfig{
		Brokers: kafka.Brokers,
		Topic:   PostCreatedTopic,
		GroupID: consumerGroupID,
	})
}

func RunPostCreatedConsumer(ctx context.Context, consumerGroupID string, svc service.IFanoutService) error {
	r := NewPostCreatedReader(consumerGroupID)
	defer func() { _ = r.Close() }()

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			return err
		}

		var event events.PostCreated
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Printf("kafka post created: invalid json partition=%d offset=%d: %v", m.Partition, m.Offset, err)
			continue
		}

		postID := strings.TrimSpace(event.PostID)
		ownerID := strings.TrimSpace(event.OwnerID)
		content := strings.TrimSpace(event.Content)
		if postID == "" || ownerID == "" || content == "" {
			log.Printf("kafka post created: missing postID, ownerID, or content partition=%d offset=%d", m.Partition, m.Offset)
			continue
		}

		mediaURL := ""
		if event.ImageURL != nil {
			mediaURL = strings.TrimSpace(*event.ImageURL)
		}

		err = svc.ProcessPostCreated(ctx, service.PostCreatedInput{
			PostID:   postID,
			Content:  content,
			MediaURL: mediaURL,
			OwnerID:  ownerID,
		})
		if err != nil {
			log.Printf("kafka post created: process failed postID=%s ownerID=%s: %v", postID, ownerID, err)
			continue
		}

		log.Printf("kafka post created: ok postID=%s ownerID=%s partition=%d offset=%d", postID, ownerID, m.Partition, m.Offset)
	}
}
