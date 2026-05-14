package consumers

import (
	"context"
	"log"

	kgo "github.com/segmentio/kafka-go"
	"github.com/sriraghariharan/leaf-fanout-service/internal/kafka"
)


const PostCreatedTopic = "post.created"


//Create a reader aka consumer for the topic "post.created"
func NewPostCreatedReader(consumerGroupID string) *kgo.Reader {

	// establish a persistent connection to the kafka broker server
	return kgo.NewReader(kgo.ReaderConfig{
		Brokers: kafka.Brokers,
		Topic:   PostCreatedTopic,
		GroupID: consumerGroupID,
	})
}

/*
	* Run the consumer for the topic "post.created"
	* Used to consume messages from the kafka broker
*/
func RunPostCreatedConsumer(ctx context.Context, consumerGroupID string) error {
	r := NewPostCreatedReader(consumerGroupID)

	defer func() { _ = r.Close() }()

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			return err
		}
		log.Printf("kafka post created: partition=%d offset=%d value=%s", m.Partition, m.Offset, m.Value)
	}
}
