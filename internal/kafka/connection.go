package kafka

import (
	"fmt"
	"log"
	"os"
	"strings"

	kgo "github.com/segmentio/kafka-go"
)

const envKafkaBrokers = "KAFKA_BROKERS"

var (
	// Brokers is the parsed bootstrap broker list after a successful Connect.
	Brokers []string
	// Writer is the shared Kafka producer after a successful Connect.
	// Callers set the destination topic per message via kafka.Message{Topic: ...}.
	Writer *kgo.Writer
)

// ConnectKafka parses KAFKA_BROKERS (comma-separated host:port list), verifies
// connectivity by dialing the first broker, and constructs the shared Writer.
func ConnectKafka() error {
	raw := os.Getenv(envKafkaBrokers)
	if raw == "" {
		return fmt.Errorf("%s is not set", envKafkaBrokers)
	}

	Brokers = splitBrokers(raw)
	if len(Brokers) == 0 {
		return fmt.Errorf("%s must contain at least one host:port", envKafkaBrokers)
	}

	/*
		* Startup connectivity check
		* Like a health check for the kafka broker
		* Fail fast if the broker is not reachable
	*/
	conn, err := kgo.Dial("tcp", Brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka: %w", err)
	}
	_ = conn.Close()

	/*
		* Create a writer aka producer for the topic "posts"
		* this will be used to send messages to the kafka broker
	*/
	Writer = &kgo.Writer{
		Addr:     kgo.TCP(Brokers...),
		Balancer: &kgo.Hash{},
	}

	log.Printf("kafka: connected (brokers=%v)", Brokers)
	return nil
}

// CloseKafka shuts down the shared Writer. Safe to call at shutdown.
func CloseKafka() {
	if Writer != nil {
		_ = Writer.Close()
		Writer = nil
	}
}

/*
	* Split the brokers string into a slice of strings
	* Each string is a broker host:port
*/
func splitBrokers(s string) []string {
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
