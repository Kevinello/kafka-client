package kafka

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/alitto/pond"
	"github.com/dlclark/regexp2"
	"github.com/go-logr/logr"
	"github.com/gofrs/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/samber/lo"
	"github.com/segmentio/kafka-go"
)

// GetTopicsFunc way to get needed topic(implemented by user)
//
//	@return topics []string
//	@return err error
//	@author kevineluo
//	@update 2023-02-24 01:53:19
type GetTopicsFunc func() (topics []string, err error)

// MessageHandler function which handles received messages from the Kafka broker.
//
//	@param msg *kafkago.Message
//	@param consumer *Consumer
//	@return err error
//	@author kevineluo
//	@update 2023-02-24 11:34:44
type MessageHandler func(msg *kafka.Message, consumer *Consumer) (err error)

// Consumer struct holds data related to the consumer
//
//	@author kevineluo
//	@update 2023-02-24 01:53:37
type Consumer struct {
	ConsumerConfig

	id         string           // ID of consumer
	index      int              // Index of message from start offset
	topics     []string         // Topics to consume
	reader     *kafka.Reader    // Reader for consume multiple topics
	workerPool *pond.WorkerPool // Pool of worker threads for processing messages

	lastConsumeTime    time.Time // Last received data, if exceeded:
	lastCheckTopicTime time.Time // Time when topics were last checked
	errCount           int       // count errors from consuming message, when it reach

	close  chan error // channel receive close signal and reason(error)
	logger *logger    // logger implement kafkago.Logger and logr.LogSinker
}

// ConsumerConfig configuration object used to create new instances of Consumer
//
//	@author kevineluo
//	@update 2023-03-15 03:01:48
type ConsumerConfig struct {
	bootstrap                string        // kafka bootstrap
	groupID                  string        // Group ID of consumer
	maxMsgInterval           time.Duration // If no message received after [MaxMsgInterval] seconds then restart Consumer
	MaxConsumeGoroutines     int           // Maximum number of goroutine for subscribing to topics
	KafkaCheckTopicPeriodSec int           // Time period for checking topics
	MaxConsumeErrorCount     int           // max error count for consuming messages
	getTopics                GetTopicsFunc // Function used to check topics
	logrLogger               *logr.Logger  // logger implement logr.LogSinker
}

// Check check config and set default value
//
//	@receiver config *ConsumerConfig
//	@return err error
//	@author kevineluo
//	@update 2023-03-15 03:19:23
func (config *ConsumerConfig) Check() (err error) {
	if config.bootstrap == "" {
		config.bootstrap = "localhost:9092"
	}
	if config.getTopics == nil {
		config.getTopics = GetAllTopic(config.bootstrap)
	}
	if config.groupID == "" {
		err = multierror.Append(err, errors.New("missing groupID"))
	}
	if config.logrLogger == nil {
		logger := logr.Discard()
		config.logrLogger = &logger
	}
	if config.maxMsgInterval == 0 {
		config.maxMsgInterval = 5 * 60
	}
	if config.KafkaCheckTopicPeriodSec == 0 {
		config.KafkaCheckTopicPeriodSec = 10
	}
	if config.MaxConsumeErrorCount == 0 {
		config.MaxConsumeErrorCount = 5
	}
	if config.MaxConsumeGoroutines == 0 {
		config.MaxConsumeGoroutines = runtime.NumCPU()
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
func NewConsumer(config ConsumerConfig) (c *Consumer, err error) {
	err = config.Check()
	if err != nil {
		return
	}
	topics, err := config.getTopics()
	if err != nil {
		err = fmt.Errorf("[NewConsumer] getTopics error: %w, GroupID: %s", err, config.groupID)
		return
	} else if len(topics) == 0 {
		err = fmt.Errorf("[NewConsumer] getTopics error: %w, GroupID: %s", ErrNoTopics, config.groupID)
		return
	}

	brokers := getBrokerList(config.bootstrap)
	logger := &logger{*config.logrLogger}
	// Configures Kafka reader object using the retrieved topics, brokers and group identifier and initialized.
	reader := kafka.NewReader(kafka.ReaderConfig{
		GroupTopics: topics,
		GroupID:     config.groupID,
		Brokers:     brokers,
		Logger:      logger,
	})
	// Initializes and configures worker pool instance using the maximum conventional goroutines count.
	workerPool := pond.New(config.MaxConsumeGoroutines, 2*config.MaxConsumeGoroutines, pond.Strategy(pond.Balanced()))
	UUID := lo.Must(uuid.NewV4())
	// Instantiates and initializes the consumer instance with previous created/configured reader, topics, group id, etc.
	c = &Consumer{
		ConsumerConfig:     config,
		id:                 UUID.String(),
		index:              0,
		topics:             topics,
		reader:             reader,
		workerPool:         workerPool,
		lastConsumeTime:    time.Now(),
		lastCheckTopicTime: time.Now(),
		close:              make(chan error),
		logger:             logger,
	}

	return
}

// StartConsume start consume messages with exact MessageHandler
//
//	@receiver consumer *Consumer
//	@param ch MessageHandler
//	@return err error
//	@author kevineluo
//	@update 2023-03-14 06:46:28
func (consumer *Consumer) StartConsume(ctx context.Context, messageHandler MessageHandler) {
	subCtx, cancel := context.WithCancel(ctx)

	// when the close signal reaches, clean up all resources, call cancel
	go func() {
		reason := <-consumer.close
		if err := consumer.clean(reason); err != nil {
			consumer.logger.Error(err, "error when clean consumer")
		}
		cancel()
	}()

	// goroutine for checking if it has been too long since any data was received.
	go consumer.check(subCtx)

	// keep consuming messages
	go consumer.run(subCtx, messageHandler)

	return
}

// Close close the consumer
//
//	@receiver consumer *Consumer
//	@author kevineluo
//	@update 2023-03-15 01:52:18
func (consumer *Consumer) Close() error {
	return consumer.sendCloseSignal(fmt.Errorf("received close signal"))
}

// run keep consuming messages
//
//	@receiver consumer *Consumer
//	@param ctx context.Context
//	@param messageHandler MessageHandler
//	@author kevineluo
//	@update 2023-03-15 02:39:04
func (consumer *Consumer) run(ctx context.Context, messageHandler MessageHandler) {
	for {
		// check if consumer.ErrCount reach MaxConsumeErrorCount
		if consumer.errCount >= consumer.MaxConsumeErrorCount {
			consumer.close <- ErrTooManyConsumeError
			return
		}

		if msg, e := consumer.reader.ReadMessage(ctx); e != nil {
			consumer.logger.Error(e, "[Consumer.run] error when read message")
			consumer.errCount++
		} else {
			// successful consumption of data
			consumer.workerPool.Submit(func() {
				if e = messageHandler(&msg, consumer); e != nil {
					consumer.logger.Error(e, "[Consumer.run] error when handle message")
					consumer.errCount++
				} else {
					consumer.index++
					consumer.lastConsumeTime = time.Now()
				}
			})
		}
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
func (consumer *Consumer) checkTopics() (changed bool, err error) {
	newTopics := make([]string, 0)

	if newTopics, err = consumer.getTopics(); err != nil {
		err = fmt.Errorf("[Consumer.CheckTopics] getTopics error: %w, GroupID: %s", err, consumer.groupID)
		return
	}

	if len(newTopics) != len(consumer.topics) {
		changed = true
		return
	}

	left, right := lo.Difference(consumer.topics, newTopics)
	changed = len(left) != 0 || len(right) != 0

	return
}

// sendCloseSignal send close signal with error to Consumer, then the Consumer will start clean up and close
//
//	@receiver consumer *Consumer
//	@param reason error
//	@return err error
//	@author kevineluo
//	@update 2023-03-15 10:08:20
func (consumer *Consumer) sendCloseSignal(reason error) (err error) {
	// check if consumer.close has been closed
	select {
	case <-consumer.close:
		err = ErrClosedConsumer
		return
	default:
	}

	// try to send close signal to consumer
	select {
	case consumer.close <- reason:
		consumer.logger.Info("[Consumer.sendCloseSignal] send close signal to consumer")
	default:
		// the goroutine listen to consumer.close hasn't start in consumer.StartConsume
		err = ErrInactiveConsumer
	}

	return
}

// check check if it has been too long since any data was received / topic change
//
//	@receiver consumer *Consumer pointer to the consumer which calls this function
//	@return err error
//	@author kevineluo
//	@update 2023-02-24 11:33:25
func (consumer *Consumer) check(ctx context.Context) {
	consumer.lastCheckTopicTime = time.Now()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		select {
		case <-ctx.Done():
			consumer.logger.Info("[Consumer.Check] context canceled, stop checking")
			return
		default:
		}
		now := time.Now()
		if now.Sub(consumer.lastCheckTopicTime).Seconds() > float64(consumer.KafkaCheckTopicPeriodSec) {
			consumer.lastCheckTopicTime = time.Now()

			if len(consumer.topics) == 0 {
				consumer.sendCloseSignal(ErrNoTopics)
				return
			}

			if changed, err := consumer.checkTopics(); err != nil {
				consumer.sendCloseSignal(fmt.Errorf("Consumer.CheckTopics error: %w", err))
				return
			} else if changed {
				consumer.sendCloseSignal(ErrTopicChanged)
				return
			}
		}

		// Check if it has been too long since any data was received
		if now.Sub(consumer.lastConsumeTime) > consumer.maxMsgInterval {
			consumer.sendCloseSignal(ErrTooLongSinceLastConsume)
			return
		}
	}
}

// clean closes all opened resources / active goroutines of Consumer
// It returns an error if there were any.
//
//	@receiver consumer *Consumer
//	@return err error
//	@author kevineluo
//	@update 2023-02-24 11:46:25
func (consumer *Consumer) clean(reason error) (err error) {
	consumer.logger.Info("[Consumer.Close] consumer will be closed.", "reason", reason.Error())
	close(consumer.close)
	err = consumer.reader.Close()
	if err != nil {
		consumer.logger.Error(err, "[Consumer.Close] error when close kafka Reader", "ID", consumer.id, "index", consumer.index)
	}
	consumer.workerPool.StopAndWait()
	return
}

// GetTopicReMatch function decorator for get topics with regex match, return GetTopicsFunc
// matches found (resTopics) and an err if applicable.
//
//	@param reList []string
//	@return resTopics []string
//	@return err error
//	@author kevineluo
//	@update 2023-02-24 05:00:53
func GetTopicReMatch(reList []string, kafkaBootstrap string) GetTopicsFunc {
	return func() (resTopics []string, err error) {
		resTopics = make([]string, 0)
		topics, err := getTopics(getBrokerList(kafkaBootstrap)[0])
		if err != nil {
			return
		}
		for _, topic := range topics {
			for _, re := range reList {
				expr := regexp2.MustCompile(re, 0)
				if matched, err := expr.MatchString(topic); err == nil && matched {
					resTopics = append(resTopics, topic)
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
func GetAllTopic(kafkaBootstrap string) GetTopicsFunc {
	return func() (topics []string, err error) {
		topics, err = getTopics(getBrokerList(kafkaBootstrap)[0])
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
