package consumers

import (
	"context"
	"log"

	kgo "github.com/segmentio/kafka-go"
	"github.com/sriraghariharan/leaf-fanout-service/internal/kafka"
)


const PostUpdatedTopic = "post.updated"


//Create a reader aka consumer for the topic "post.updated"
func NewPostUpdatedReader(consumerGroupID string) *kgo.Reader {

	// establish a persistent connection to the kafka broker server
	return kgo.NewReader(kgo.ReaderConfig{
		Brokers: kafka.Brokers,
		Topic:   PostUpdatedTopic,
		GroupID: consumerGroupID,
	})
}

/*
	* Run the consumer for the topic "post.updated"
	* Used to consume messages from the kafka broker
*/
func RunPostUpdatedConsumer(ctx context.Context, consumerGroupID string) error {
	r := NewPostUpdatedReader(consumerGroupID)

	defer func() { _ = r.Close() }()

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			return err
		}
		log.Printf("kafka post updated: partition=%d offset=%d value=%s", m.Partition, m.Offset, m.Value)
	}
}
