// Package rabbitmq exposes a process-wide singleton AMQP connection and one
// long-lived channel. Call Connect once at startup, defer Close at shutdown,
// then read Conn / Ch from anywhere in the service.
package rabbitmq

import (
	"fmt"
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

const envRabbitMQURL = "RABBITMQ_CONNECTION_STRING"

var (
	// Conn is the shared AMQP connection after a successful Connect.
	Conn *amqp.Connection
	// Ch is the shared AMQP channel after a successful Connect.
	Ch *amqp.Channel
)

// Connect dials RabbitMQ using RABBITMQ_CONNECTION_STRING and opens one
// long-lived channel on success.
func ConnectRabbitMQ() error {
	url := os.Getenv(envRabbitMQURL)
	if url == "" {
		return fmt.Errorf("%s is not set", envRabbitMQURL)
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("dial rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("open channel: %w", err)
	}

	Conn = conn
	Ch = ch
	log.Println("rabbitmq: connected")
	return nil
}

// Close releases the channel and connection. Safe to call at shutdown.
func CloseRabbitMQ() {
	if Ch != nil {
		_ = Ch.Close()
		Ch = nil
	}
	if Conn != nil {
		_ = Conn.Close()
		Conn = nil
	}
}
