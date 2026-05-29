package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	kgo "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

const envKafkaBrokers = "KAFKA_BROKERS"

var (
	// Brokers is the parsed bootstrap broker list after a successful Connect.
	Brokers []string
	// Dialer is the shared connection dialer (plain TCP locally, TLS+SASL on Aiven).
	Dialer *kgo.Dialer
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

	mode := resolveMode()
	dialer, err := buildDialer(mode)
	if err != nil {
		return err
	}
	Dialer = dialer

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := Dialer.DialContext(ctx, "tcp", Brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka: %w", err)
	}
	_ = conn.Close()

	Writer = &kgo.Writer{
		Addr:     kgo.TCP(Brokers...),
		Balancer: &kgo.Hash{},
	}
	if mode == "aiven" {
		Writer.Transport = &kgo.Transport{
			TLS:  Dialer.TLS,
			SASL: Dialer.SASLMechanism,
		}
	}

	log.Printf("kafka: connected (mode=%s brokers=%v)", mode, Brokers)
	return nil
}

// CloseKafka shuts down the shared Writer. Safe to call at shutdown.
func CloseKafka() {
	if Writer != nil {
		_ = Writer.Close()
		Writer = nil
	}
}

func resolveMode() string {
	explicit := strings.ToLower(strings.TrimSpace(os.Getenv("KAFKA_MODE")))
	if explicit == "local" || explicit == "aiven" {
		return explicit
	}
	if strings.TrimSpace(os.Getenv("KAFKA_SASL_USERNAME")) != "" &&
		os.Getenv("KAFKA_SASL_PASSWORD") != "" {
		return "aiven"
	}
	return "local"
}

func loadCaPEM() ([]byte, error) {
	if path := strings.TrimSpace(os.Getenv("KAFKA_SSL_CA_PATH")); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read KAFKA_SSL_CA_PATH: %w", err)
		}
		return data, nil
	}
	if inline := strings.TrimSpace(os.Getenv("KAFKA_SSL_CA")); inline != "" {
		return []byte(strings.ReplaceAll(inline, "\\n", "\n")), nil
	}
	return nil, fmt.Errorf("aiven mode requires KAFKA_SSL_CA_PATH or KAFKA_SSL_CA")
}

func buildSASLMechanism() (sasl.Mechanism, error) {
	username := strings.TrimSpace(os.Getenv("KAFKA_SASL_USERNAME"))
	password := os.Getenv("KAFKA_SASL_PASSWORD")
	if username == "" {
		return nil, fmt.Errorf("KAFKA_SASL_USERNAME is required in aiven mode")
	}
	if password == "" {
		return nil, fmt.Errorf("KAFKA_SASL_PASSWORD is required in aiven mode")
	}

	mechanism := strings.ToLower(strings.TrimSpace(os.Getenv("KAFKA_SASL_MECHANISM")))
	if mechanism == "" {
		mechanism = "scram-sha-256"
	}

	switch mechanism {
	case "scram-sha-256":
		return scram.Mechanism(scram.SHA256, username, password)
	case "scram-sha-512":
		return scram.Mechanism(scram.SHA512, username, password)
	case "plain":
		return plain.Mechanism{Username: username, Password: password}, nil
	default:
		return nil, fmt.Errorf("unsupported KAFKA_SASL_MECHANISM: %s", mechanism)
	}
}

func buildDialer(mode string) (*kgo.Dialer, error) {
	if mode == "local" {
		return &kgo.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
		}, nil
	}

	caPEM, err := loadCaPEM()
	if err != nil {
		return nil, err
	}

	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}

	mechanism, err := buildSASLMechanism()
	if err != nil {
		return nil, err
	}

	return &kgo.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		TLS:           &tls.Config{RootCAs: caPool, MinVersion: tls.VersionTLS12},
		SASLMechanism: mechanism,
	}, nil
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
