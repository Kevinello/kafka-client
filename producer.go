package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"github.com/samber/lo"
	"github.com/segmentio/kafka-go"
)

// Producer struct holds data related to the producer
//
//	@author kevineluo
//	@update 2023-03-15 10:30:44
type Producer struct {
	ProducerConfig

	id     string        // ID of producer
	writer *kafka.Writer // Writer for producing messages

	context context.Context
	cancel  context.CancelCauseFunc
	logger  *logger // logger implement kafkago.Logger and logr.LogSinker

	writeCount int // write messages count
}

// NewProducer creates a new Kafka producer.
//
//	@param kafkaBootstrap string
//	@param logrLogger logr.Logger
//	@return p *Producer
//	@return err error
//	@author kevineluo
//	@update 2023-03-15 10:52:06
//
// NewProducer creates a new producer with the given config
// A producer is a wrapper of kafka writer with some additional features
// such as topic auto creation, message batching, etc
// Note that the producer is not thread-safe
// If you want to use one producer in multiple goroutines, you should do the synchronization by yourself
func NewProducer(ctx context.Context, config ProducerConfig) (p *Producer, err error) {
	// validate the config
	if err = config.Validate(); err != nil {
		return
	}

	// create a sub-context with a cancel function to be used for the producer
	subCtx, cancel := context.WithCancelCause(ctx)

	// create a logger
	logger := &logger{*config.Logger}

	// log the start of this producer
	logger.Info("[NewProducer] start new producer with config", "config", config)

	// split the bootstrap servers into a slice
	brokers := strings.Split(config.Bootstrap, ",")

	// create a kafka writer config
	writerConfig := kafka.WriterConfig{
		Brokers: brokers,
		Async:   config.Async,
	}

	// set the logger if in verbose mode
	if config.Verbose {
		writerConfig.Logger = logger
	}

	// create a kafka writer
	writer := kafka.NewWriter(writerConfig)

	// set the writer to allow auto topic creation
	// if we don't want to ignore the missing topic
	writer.AllowAutoTopicCreation = config.AllowAutoTopicCreation

	// create a producer
	p = &Producer{
		ProducerConfig: config,
		id:             lo.Must(uuid.NewV4()).String(),
		writer:         writer,

		context: subCtx,
		cancel:  cancel,
		logger:  logger,

		writeCount: 0,
	}

	// start the cleanup goroutine
	go p.cleanup()

	return
}

// Close close the producer
//
//	@receiver producer *Producer
//	@return error
//	@author kevineluo
//	@update 2023-03-15 02:43:18
func (producer *Producer) Close() error {
	if producer.Closed() {
		return ErrClosedConsumer
	}
	producer.cancel(fmt.Errorf("received close signal"))
	return nil
}

// WriteMessages write messages to kafka, notice that all messages should set their own topic
//
//	@receiver producer *Producer
//	@param ctx context.Context
//	@param msgs ...kafka.Message
//	@return err error
//	@author kevineluo
//	@update 2023-03-29 02:46:03
func (producer *Producer) WriteMessages(ctx context.Context, msgs ...kafka.Message) (err error) {
	for i := 0; i < len(msgs); i++ {
		e := producer.writer.WriteMessages(ctx, msgs...)
		if e == nil {
			producer.writeCount += len(msgs)
			break
		} else if errors.Is(e, kafka.LeaderNotAvailable) || errors.Is(e, context.DeadlineExceeded) {
			time.Sleep(time.Millisecond * 250)
			continue
		} else {
			return e
		}
	}
	return
}

// Closed check if the Producer is Closed
//
//	@receiver producer *Producer
//	@return bool
//	@author kevineluo
//	@update 2023-03-30 05:11:40
func (producer *Producer) Closed() bool {
	select {
	case <-producer.context.Done():
		return true
	default:
		return false
	}
}

// cleanup clean closes all opened resources of Producer
//
//	@receiver producer *Producer
//	@return err error
//	@author kevineluo
//	@update 2023-03-30 05:16:44
func (producer *Producer) cleanup() (err error) {
	<-producer.context.Done()
	return producer.writer.Close()
}
