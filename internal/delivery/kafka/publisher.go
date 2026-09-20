package kafka

import (
	"context"
	"food-ingestion-service/internal/domain"
	"github.com/segmentio/kafka-go"
)

type kafkaPublisher struct {
	writer *kafka.Writer
}

func NewKafkaPublisher(brokers []string) domain.KafkaPublisher {
	return &kafkaPublisher{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Balancer: &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
		},
	}
}

func (p *kafkaPublisher) PublishEvent(ctx context.Context, topic string, key string, payload []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: payload,
	})
}
