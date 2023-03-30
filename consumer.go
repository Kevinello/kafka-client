// Package kafka Manage Kafka Client
//
//	@update 2023-03-28 02:01:25
package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/alitto/pond"
	"github.com/dlclark/regexp2"
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/gofrs/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/samber/lo"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Consumer struct holds data related to the consumer
//
//	@author kevineluo
//	@update 2023-02-24 01:53:37
type Consumer struct {
	ConsumerConfig

	id         string           // ID of consumer
	reader     *kafka.Reader    // Reader for consume multiple topics
	workerPool *pond.WorkerPool // Pool of worker threads for processing messages

	context          context.Context
	cancel           context.CancelCauseFunc
	consumeErrorChan chan error  // channel receive error during consuming messages, when error count reach ConsumerConfig.MaxConsumeErrorCount, consumer will be closed
	noMessageTimer   *time.Timer // timer for checking if it's too long since last consumed message
	logger           *logger     // logger implement kafkago.Logger and logr.LogSinker

	brokers     []string
	topics      []string // Topics to consume
	deltaOffset int      // message count from start offset
}

// ConsumerConfig configuration object used to create new instances of Consumer
//
//	@author kevineluo
//	@update 2023-03-15 03:01:48
type ConsumerConfig struct {
	Bootstrap            string         // kafka bootstrap, default: "localhost:9092"
	GroupID              string         // Group ID of consumer
	MaxMsgInterval       time.Duration  // If no message received after [MaxMsgInterval] seconds then restart Consumer, default: 300 seconds
	SyncTopicInterval    time.Duration  // Interval for consumer to sync topics, default: 15 seconds
	MaxConsumeGoroutines int            // Maximum number of goroutine for subscribing to topics, default: runtime.NumCPU()
	MaxConsumeErrorCount int            // max error count for consuming messages, default: 5
	MessageHandler       MessageHandler // function which handles received messages from the Kafka broker.
	GetTopics            GetTopicsFunc  // Function used to sync topics, default: GetAllTopic
	Logger               *logr.Logger   // logger implement logr.LogSinker, default: zapr.Logger
	LogLevel             int            // used when use default Logger, follow the zap style level(https://pkg.go.dev/go.uber.org/zap@v1.24.0/zapcore#Level), setting the log level for zapr.Logger(config.logLevel should be in range[-1, 5], default: 0 -- InfoLevel)
}

// GetTopicsFunc way to get needed topic(implemented and provided by user)
//
//	@return topics []string
//	@return err error
//	@author kevineluo
//	@update 2023-03-28 07:16:54
type GetTopicsFunc func(broker string) (topics []string, err error)

// MessageHandler function which handles received messages from the Kafka broker.
//
//	@param msg *kafka.Message
//	@param consumer *Consumer
//	@return err error
//	@author kevineluo
//	@update 2023-03-28 07:16:44
type MessageHandler func(msg *kafka.Message, consumer *Consumer) (err error)

// Validate check config and set default value
//
//	@receiver config *ConsumerConfig
//	@return err error
//	@author kevineluo
//	@update 2023-03-15 03:19:23
func (config *ConsumerConfig) Validate() (err error) {
	if config.Bootstrap == "" {
		config.Bootstrap = "localhost:9092"
	}
	if config.GroupID == "" {
		err = multierror.Append(err, errors.New("missing GroupID"))
	}
	if config.MaxMsgInterval == 0 {
		config.MaxMsgInterval = 5 * 60 * time.Second
	}
	if config.SyncTopicInterval == 0 {
		config.SyncTopicInterval = 15 * time.Second
	}
	if config.MaxConsumeGoroutines == 0 {
		config.MaxConsumeGoroutines = runtime.NumCPU()
	}
	if config.MaxConsumeErrorCount == 0 {
		config.MaxConsumeErrorCount = 5
	}
	if config.MessageHandler == nil {
		err = multierror.Append(err, errors.New("missing MessageHandler"))
	}
	if config.GetTopics == nil {
		config.GetTopics = GetAllTopic()
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
			err = fmt.Errorf("[ConsumerConfig.Check] found invalid ConsumerConfig.LogLevel: %d, ConsumerConfig.LogLevel should be in range[-1, 5]", config.LogLevel)
			return
		}
		zapLogger := zap.New(zapcore.NewCore(zapcore.NewConsoleEncoder(cfg.EncoderConfig), zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout)), level))
		logger := zapr.NewLogger(zapLogger)
		config.Logger = &logger
	}

	return
}

// NewConsumer creates a new Kafka consumer.
//
//	@param kafkaBootstrap string
//	@param groupID string
//	@param getTopics GetTopicsFunc
//	@return c *Consumer
//	@return err error
//	@author kevineluo
//	@update 2023-03-14 01:12:16
func NewConsumer(ctx context.Context, config ConsumerConfig) (c *Consumer, err error) {
	if err = config.Validate(); err != nil {
		return
	}
	subCtx, cancel := context.WithCancelCause(ctx)
	logger := &logger{*config.Logger}
	brokers := strings.Split(config.Bootstrap, ",")
	topics, err := config.GetTopics(brokers[0])
	if err != nil {
		err = fmt.Errorf("[NewConsumer] getTopics error: %w, GroupID: %s", err, config.GroupID)
		return
	} else if len(topics) == 0 {
		err = fmt.Errorf("[NewConsumer] getTopics error: %w, GroupID: %s", ErrNoTopics, config.GroupID)
		return
	}
	config.Logger.Info("[NewConsumer] first time get topics success", "topics", topics)

	// Configures Kafka reader object using the retrieved topics, brokers and group identifier and initialized.
	reader := kafka.NewReader(kafka.ReaderConfig{
		GroupTopics: topics,
		GroupID:     config.GroupID,
		Brokers:     brokers,
		Logger:      logger,
	})
	// Instantiates and initializes the consumer instance with previous created/configured reader, topics, group id, etc.
	c = &Consumer{
		ConsumerConfig: config,
		id:             lo.Must(uuid.NewV4()).String(),
		reader:         reader,
		workerPool:     pond.New(config.MaxConsumeGoroutines, 2*config.MaxConsumeGoroutines, pond.Strategy(pond.Balanced())),

		context:          subCtx,
		cancel:           cancel,
		consumeErrorChan: make(chan error),
		noMessageTimer:   time.NewTimer(config.MaxMsgInterval),
		logger:           logger,

		brokers:     brokers,
		topics:      topics,
		deltaOffset: 0,
	}

	go c.cleanup()

	// goroutine for checking error / topic change etc.
	go c.check()

	// goroutine for keep consuming messages
	go c.run()

	return
}

// Close manually close the consumer
//
//	@receiver consumer *Consumer
//	@author kevineluo
//	@update 2023-03-15 01:52:18
func (consumer *Consumer) Close() error {
	if consumer.closed() {
		return ErrClosedConsumer
	}
	consumer.cancel(fmt.Errorf("received close signal"))
	return nil
}

// run keep consuming messages
//
//	@receiver consumer *Consumer
//	@param ctx context.Context
//	@param messageHandler MessageHandler
//	@author kevineluo
//	@update 2023-03-15 02:39:04
func (consumer *Consumer) run() {
	defer close(consumer.consumeErrorChan)
	for {
		select {
		case <-consumer.context.Done():
			consumer.logger.Info("[Consumer.run] context canceled, stop consuming messages", "cause", context.Cause(consumer.context))
			return
		default:
			if msg, e := consumer.reader.ReadMessage(consumer.context); e != nil && e != context.Canceled {
				consumer.logger.Error(e, "[Consumer.run] error when read message")
				consumer.consumeErrorChan <- e
			} else {
				// successful consumption of data
				if !consumer.workerPool.Stopped() {
					consumer.workerPool.Submit(func() {
						if e = consumer.MessageHandler(&msg, consumer); e != nil && e != context.Canceled {
							consumer.logger.Error(e, "[Consumer.run] error when handle message")
							consumer.consumeErrorChan <- e
						} else {
							consumer.deltaOffset++
							consumer.noMessageTimer.Reset(consumer.MaxMsgInterval)
						}
					})
				}
			}
		}
	}
}

// check check if it has been too long since any data was received / topic change
//
//	@receiver consumer *Consumer pointer to the consumer which calls this function
//	@return err error
//	@author kevineluo
//	@update 2023-02-24 11:33:25
func (consumer *Consumer) check() {
	syncTopicTicker := time.NewTicker(consumer.SyncTopicInterval)
	defer syncTopicTicker.Stop()

	errList := make([]error, 0)

	for {
		select {
		case <-consumer.context.Done():
			// wait for context cancellation
			consumer.logger.Info("[Consumer.check] context canceled, stop checking")
			return
		case <-consumer.noMessageTimer.C:
			// too long since last consumed message, reset kafka reader(connection)
			consumer.logger.Info("[Consumer.check] too long since last consumed message, about to reset kafka connection")
			consumer.reader = kafka.NewReader(kafka.ReaderConfig{
				GroupTopics: consumer.topics,
				GroupID:     consumer.GroupID,
				Brokers:     consumer.brokers,
				Logger:      consumer.logger,
			})
			consumer.noMessageTimer.Reset(consumer.MaxMsgInterval)
		case <-syncTopicTicker.C:
			// tick to sync topics
			if topics, changed, err := consumer.checkTopics(); err != nil {
				consumer.cancel(fmt.Errorf("[Consumer.check] error when check topics: %w", err))
				return
			} else if changed {
				consumer.topics = topics
				// topic change detected, reset kafka reader
				consumer.logger.Info("[Consumer.check] topic change detected, about to reset kafka connection")
				consumer.reader = kafka.NewReader(kafka.ReaderConfig{
					GroupTopics: topics,
					GroupID:     consumer.GroupID,
					Brokers:     consumer.brokers,
					Logger:      consumer.logger,
				})
			}
		case err := <-consumer.consumeErrorChan:
			errList = append(errList, err)
			if len(errList) >= consumer.MaxConsumeErrorCount {
				consumer.cancel(ErrTooManyConsumeError)
				return
			}
		}
	}
}

// cleanup closes all opened resources of Consumer
// It returns an error if there were any.
//
//	@receiver consumer *Consumer
//	@return err error
//	@author kevineluo
//	@update 2023-02-24 11:46:25
func (consumer *Consumer) cleanup() (err error) {
	<-consumer.context.Done()
	err = consumer.reader.Close()
	if err != nil {
		consumer.logger.Error(err, "[Consumer.cleanup] error when close kafka Reader", "ID", consumer.id, "delta offset", consumer.deltaOffset)
	}
	// wait for workerpool to handle rest messages
	consumer.workerPool.StopAndWaitFor(30 * time.Second)

	return
}

// closed check if the Consumer is closed
//
//	@receiver consumer *Consumer
//	@return bool
//	@author kevineluo
//	@update 2023-03-30 05:11:33
func (consumer *Consumer) closed() bool {
	select {
	case <-consumer.context.Done():
		return true
	default:
		return false
	}
}

// checkTopics checkTopics is used to check if topic list has been changed.
// GetTopics() function is called to get the topics list and then sorted to make sure both lists are in same order. Length of both lists is compared, and if different return true.
// Otherwise loop through each list and compare each element for equality.
//
//	@receiver consumer *Consumer
//	@return changed bool
//	@return err error
//	@author kevineluo
//	@update 2023-02-24 10:31:34
func (consumer *Consumer) checkTopics() (topic []string, changed bool, err error) {
	topic = make([]string, 0)

	if topic, err = consumer.GetTopics(consumer.brokers[0]); err != nil {
		err = fmt.Errorf("[Consumer.CheckTopics] getTopics error: %w, GroupID: %s", err, consumer.GroupID)
		return
	}

	if len(topic) != len(consumer.topics) {
		changed = true
		return
	}

	left, right := lo.Difference(consumer.topics, topic)
	changed = len(left) != 0 || len(right) != 0

	return
}

// GetTopicReMatch function decorator for get topics with regex match, return GetTopicsFunc
// matches found (resTopics) and an err if applicable.
//
//	@param reList []string
//	@return GetTopicsFunc
//	@author kevineluo
//	@update 2023-03-29 03:22:56
func GetTopicReMatch(reList []string) GetTopicsFunc {
	return func(broker string) (topics []string, err error) {
		topics = make([]string, 0)
		allTopics, err := getTopics(broker)
		if err != nil {
			return
		}
		for _, topic := range allTopics {
			for _, re := range reList {
				expr := regexp2.MustCompile(re, 0)
				if matched, err := expr.MatchString(topic); err == nil && matched {
					topics = append(topics, topic)
				}
			}
		}
		return
	}
}

// GetAllTopic function decorator for get all topics, return GetTopicsFunc
//
//	@param kafkaBootstrap string
//	@return GetTopicsFunc
//	@author kevineluo
//	@update 2023-03-15 03:14:57
func GetAllTopic() GetTopicsFunc {
	return func(broker string) (topics []string, err error) {
		topics, err = getTopics(broker)
		return
	}
}

func getTopics(broker string) (topics []string, err error) {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		err = fmt.Errorf("[getTopics] error when connect to kafka: %w", err)
		return
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		err = fmt.Errorf("[getTopics] error when ReadPartitions from kafka: %w", err)
		return
	}

	m := map[string]struct{}{}

	for _, p := range partitions {
		m[p.Topic] = struct{}{}
	}
	topics = lo.Keys(m)
	return
}
