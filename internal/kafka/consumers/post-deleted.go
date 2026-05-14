package consumers

import (
	"context"
	"log"

	kgo "github.com/segmentio/kafka-go"
	"github.com/sriraghariharan/leaf-fanout-service/internal/kafka"
)


const PostDeletedTopic = "post.deleted"


//Create a reader aka consumer for the topic "post.deleted"
func NewPostDeletedReader(consumerGroupID string) *kgo.Reader {

	// establish a persistent connection to the kafka broker server
	return kgo.NewReader(kgo.ReaderConfig{
		Brokers: kafka.Brokers,
		Topic:   PostDeletedTopic,
		GroupID: consumerGroupID,
	})
}

/*
	* Run the consumer for the topic "post.deleted"
	* Used to consume messages from the kafka broker
*/
func RunPostDeletedConsumer(ctx context.Context, consumerGroupID string) error {
	r := NewPostDeletedReader(consumerGroupID)

	defer func() { _ = r.Close() }()

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			return err
		}
		log.Printf("kafka post deleted: partition=%d offset=%d value=%s", m.Partition, m.Offset, m.Value)
	}
}
