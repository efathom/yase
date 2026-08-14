package broker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)

// EnsureTopic creates a Kafka topic with the given number of partitions and
// replication factor if it doesn't already exist. Safe to call multiple times.
func EnsureTopic(ctx context.Context, brokers []string, topic string, partitions, replicationFactor int) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers configured")
	}
	if partitions <= 0 {
		partitions = 6
	}
	if replicationFactor <= 0 {
		replicationFactor = 1
	}

	// Connect to any broker to create the topic
	addr := brokers[0]
	conn, err := kafka.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	// Find the controller broker (required for topic creation)
	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("controller: %w", err)
	}
	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	controllerConn, err := kafka.Dial("tcp", controllerAddr)
	if err != nil {
		return fmt.Errorf("dial controller %s: %w", controllerAddr, err)
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	})
	if err != nil {
		// Only "already exists" is benign; propagate everything else
		// (auth failures, invalid replication factor, timeouts, etc.).
		if errors.Is(err, kafka.TopicAlreadyExists) {
			return nil
		}
		return fmt.Errorf("create topic %q: %w", topic, err)
	}

	return nil
}
