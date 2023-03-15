package kafka

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/gofrs/uuid"
	"github.com/samber/lo"
	"github.com/segmentio/kafka-go"
)

// Producer struct holds data related to the producer
//
//	@author kevineluo
//	@update 2023-03-15 10:30:44
type Producer struct {
	id     string        // ID of producer
	writer *kafka.Writer // Writer for producing messages

	logger     *logger // logger implement kafkago.Logger and logr.LogSinker
	writeCount int     // write messages count
}

// NewProducer creates a new Kafka producer.
//
//	@param kafkaBootstrap string
//	@param logrLogger logr.Logger
//	@return p *Producer
//	@return err error
//	@author kevineluo
//	@update 2023-03-15 10:52:06
func NewProducer(kafkaBootstrap string, async bool, logrLogger logr.Logger) (p *Producer, err error) {
	brokers := getBrokerList(kafkaBootstrap)
	logger := &logger{logrLogger}
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokers,
		Logger:  logger,
		Async:   async,
	})
	UUID := lo.Must(uuid.NewV4())
	p = &Producer{
		id:     UUID.String(),
		writer: writer,
		logger: logger,
	}
	return
}

// WriteMessage write a message to kafka
//
//	@receiver producer *Producer
//	@param ctx context.Context
//	@param message *Message
//	@return err error
//	@author kevineluo
//	@update 2023-03-15 11:32:10
func (producer *Producer) WriteMessage(ctx context.Context, topic string, key, value []byte, headers []kafka.Header) (err error) {
	msg := kafka.Message{
		Topic:   topic,
		Headers: headers,
		Key:     key,
		Value:   value,
	}
	err = producer.writer.WriteMessages(ctx, msg)
	producer.writeCount++
	return
}

// Close close the producer
//
//	@receiver producer *Producer
//	@return error
//	@author kevineluo
//	@update 2023-03-15 02:43:18
func (producer *Producer) Close() error {
	return producer.clean(fmt.Errorf("received close signal"))
}

// clean closes all opened resources / active goroutines of Producer
//
//	@receiver producer *Producer
//	@return err error
//	@author kevineluo
//	@update 2023-03-15 02:40:39
func (producer *Producer) clean(reason error) (err error) {
	producer.logger.Info("[Consumer.Close] consumer will be closed.", "reason", reason.Error())
	return producer.writer.Close()
}
