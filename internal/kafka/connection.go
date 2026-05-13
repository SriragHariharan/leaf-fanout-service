// Package kafka exposes the process-wide bootstrap broker list and a shared
// Kafka producer (Writer). Call Connect once at startup, defer Close at
// shutdown, then read Brokers / Writer from anywhere in the service.
//
// Consumers (kafka.Reader) are per-topic and per-consumer-group, so callers
// create them where needed using the Brokers slice.
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

	conn, err := kgo.Dial("tcp", Brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka: %w", err)
	}
	_ = conn.Close()

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
