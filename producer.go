package kafka

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/gofrs/uuid"
	"github.com/samber/lo"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Producer struct holds data related to the producer
//
//	@author kevineluo
//	@update 2023-03-15 10:30:44
type Producer struct {
	ProducerConfig

	id     string        // ID of producer
	writer *kafka.Writer // Writer for producing messages

	logger *logger // logger implement kafkago.Logger and logr.LogSinker

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
func NewProducer(config ProducerConfig) (p *Producer, err error) {
	err = config.Validate()
	if err != nil {
		return
	}
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
		logger:         logger,
		writeCount:     0,
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

// WriteMessages write messages to kafka, notice that all messages should set their own topic
//
//	@receiver producer *Producer
//	@param ctx context.Context
//	@param msgs ...kafka.Message
//	@return err error
//	@author kevineluo
//	@update 2023-03-29 02:46:03
func (producer *Producer) WriteMessages(ctx context.Context, msgs ...kafka.Message) (err error) {
	err = producer.writer.WriteMessages(ctx, msgs...)
	producer.writeCount += len(msgs)
	return
}

// Close close the producer
//
//	@receiver producer *Producer
//	@return error
//	@author kevineluo
//	@update 2023-03-15 02:43:18
func (producer *Producer) Close() error {
	return producer.clean()
}

// clean closes all opened resources / active goroutines of Producer
//
//	@receiver producer *Producer
//	@return err error
//	@author kevineluo
//	@update 2023-03-15 02:40:39
func (producer *Producer) clean() (err error) {
	return producer.writer.Close()
}
