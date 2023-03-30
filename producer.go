package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/gofrs/uuid"
	"github.com/samber/lo"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const createTopicMaxRetries = 3

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

// ProducerConfig configuration object used to create new instances of Producer
//
//	@author kevineluo
//	@update 2023-03-15 03:01:48
type ProducerConfig struct {
	Bootstrap          string       // kafka bootstrap, default: "localhost:9092"
	Async              bool         // determine synchronously / asynchronously write messages, default: false
	IgnoreMissingTopic bool         // when it's false, producer will automate create missing topic before publication, else it will return an error when meeting missing topic, default: false
	Logger             *logr.Logger // logger implement logr.LogSinker, default: zapr.Logger
	LogLevel           int          // used when Config.logger is nil, follow the zap style level(https://pkg.go.dev/go.uber.org/zap@v1.24.0/zapcore#Level), setting the log level for zapr.Logger(config.logLevel should be in range[-1, 5], default: 0 -- InfoLevel)
}

// Validate check config and set default value
//
//	@receiver config *ProducerConfig
//	@return err error
//	@author kevineluo
//	@update 2023-03-15 03:19:23
func (config *ProducerConfig) Validate() (err error) {
	if config.Bootstrap == "" {
		config.Bootstrap = "localhost:9092"
	}
	if config.Logger == nil {
		var cfg zap.Config
		level := zapcore.Level(config.LogLevel)
		if level >= zap.DebugLevel && level <= zap.FatalLevel {
			if level == zap.DebugLevel {
				cfg = zap.NewDevelopmentConfig()
			} else {
				cfg = zap.NewProductionConfig()
			}
		} else {
			err = fmt.Errorf("[ProducerConfig.Check] found invalid ProducerConfig.LogLevel: %d, ProducerConfig.LogLevel should be in range[-1, 5]", config.LogLevel)
			return
		}
		zapLogger := zap.New(zapcore.NewCore(zapcore.NewConsoleEncoder(cfg.EncoderConfig), zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout)), level))
		logger := zapr.NewLogger(zapLogger)
		config.Logger = &logger
	}

	return
}

// NewProducer creates a new Kafka producer.
//
//	@param kafkaBootstrap string
//	@param logrLogger logr.Logger
//	@return p *Producer
//	@return err error
//	@author kevineluo
//	@update 2023-03-15 10:52:06
func NewProducer(ctx context.Context, config ProducerConfig) (p *Producer, err error) {
	if err = config.Validate(); err != nil {
		return
	}
	subCtx, cancel := context.WithCancelCause(ctx)
	brokers := strings.Split(config.Bootstrap, ",")
	logger := &logger{*config.Logger}
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokers,
		Logger:  logger,
		Async:   config.Async,
	})
	writer.AllowAutoTopicCreation = !config.IgnoreMissingTopic
	p = &Producer{
		ProducerConfig: config,
		id:             lo.Must(uuid.NewV4()).String(),
		writer:         writer,

		context: subCtx,
		cancel:  cancel,
		logger:  logger,

		writeCount: 0,
	}

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
	if producer.closed() {
		return ErrClosedConsumer
	}
	producer.cancel(fmt.Errorf("received close signal"))
	return nil
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
	for i := 0; i < createTopicMaxRetries; i++ {
		e := producer.writer.WriteMessages(ctx, kafka.Message{
			Topic:   topic,
			Headers: headers,
			Key:     key,
			Value:   value,
		})
		if e == nil {
			producer.writeCount++
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

// WriteMessages write messages to kafka, notice that all messages should set their own topic
//
//	@receiver producer *Producer
//	@param ctx context.Context
//	@param msgs ...kafka.Message
//	@return err error
//	@author kevineluo
//	@update 2023-03-29 02:46:03
func (producer *Producer) WriteMessages(ctx context.Context, msgs ...kafka.Message) (err error) {
	for i := 0; i < createTopicMaxRetries; i++ {
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

// closed check if the Producer is closed
//
//	@receiver producer *Producer
//	@return bool
//	@author kevineluo
//	@update 2023-03-30 05:11:40
func (producer *Producer) closed() bool {
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
