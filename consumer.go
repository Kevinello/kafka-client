// Package kafka Manage Kafka Client
//
//	@update 2023-03-28 02:01:25
package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/alitto/pond"
	"github.com/dlclark/regexp2"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
)

// Consumer struct holds data related to the consumer
//
//	@author kevineluo
//	@update 2023-02-24 01:53:37
type Consumer struct {
	ConsumerConfig

	id           string                  // ID of consumer
	reader       *kafka.Reader           // Reader for consume multiple topics
	readerCtx    context.Context         // Context for reader
	cancelReader context.CancelCauseFunc // Cancel function for reader
	workerPool   *pond.WorkerPool        // Pool of worker threads for processing messages

	context          context.Context
	cancel           context.CancelCauseFunc
	consumeErrorChan chan error  // channel receive error during consuming messages, when error count reach ConsumerConfig.MaxConsumeErrorCount, consumer will be closed
	noMessageTimer   *time.Timer // timer for checking if it's too long since last consumed message
	logger           *logger     // logger implement kafkago.Logger and logr.LogSinker

	brokers       []string
	topics        []string // Topics to consume
	consumeErrors []error  // collect errors during consuming messages
	deltaOffset   int      // message count from start offset
}

// GetTopicsFunc way to get needed topic(implemented and provided by user)
//
//	@return topics []string
//	@return err error
//	@author kevineluo
//	@update 2023-03-28 07:16:54
type GetTopicsFunc func(broker string, mechanism sasl.Mechanism) (topics []string, err error)

// MessageHandler function which handles received messages from the Kafka broker.
//
//	@param msg *kafka.Message
//	@param consumer *Consumer
//	@return err error
//	@author kevineluo
//	@update 2023-03-28 07:16:44
type MessageHandler func(msg *kafka.Message, consumer *Consumer) (err error)

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
	subCtx, cancel := context.WithCancelCause(ctx)

	if err = config.Validate(); err != nil {
		return
	}

	logger := &logger{*config.Logger}
	logger.Info("[NewConsumer] start new consumer with config", "config", config)
	brokers := strings.Split(config.Bootstrap, ",")
	topics, err := config.GetTopics(brokers[0], config.Mechanism)
	if err != nil {
		err = fmt.Errorf("[NewConsumer] getTopics error: %w, GroupID: %s", err, config.GroupID)
		return
	}
	logger.Info("[NewConsumer] first time get topics success", "topics", topics)

	// Configures Kafka reader object using the retrieved topics, brokers and group identifier and initialized.
	readerConfig := kafka.ReaderConfig{
		GroupTopics: topics,
		GroupID:     config.GroupID,
		Brokers:     brokers,
	}

	// set the logger if in verbose mode
	if config.Verbose {
		readerConfig.Logger = logger
	}

	// set the dialer for SASL mechanism and TLS
	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		SASLMechanism: config.Mechanism,
		TLS:           config.TLS,
	}
	readerConfig.Dialer = dialer

	var reader *kafka.Reader
	if len(topics) > 0 {
		// we can only create reader when topics is not empty
		reader = kafka.NewReader(readerConfig)
	}

	// Instantiates and initializes the consumer instance with previous created/configured reader, topics, group id, etc.
	c = &Consumer{
		ConsumerConfig: config,
		id:             uuid.New().String(),
		reader:         reader,
		workerPool:     pond.New(config.MaxConsumeGoroutines, 2*config.MaxConsumeGoroutines, pond.MinWorkers(lo.Max([]int{config.MaxConsumeGoroutines / 2, 1}))),

		context:          subCtx,
		cancel:           cancel,
		consumeErrorChan: make(chan error),
		noMessageTimer:   time.NewTimer(config.MaxMsgInterval),
		logger:           logger,

		brokers:     brokers,
		topics:      topics,
		deltaOffset: 0,
	}
	c.readerCtx, c.cancelReader = context.WithCancelCause(subCtx)

	// goroutine for cleanup resources when consumer closed
	go c.cleanup()

	// goroutine for checking error / topic change etc.
	go c.check()

	// goroutine for keep consuming messages
	go c.run()

	return
}

// CheckState check consumer state
//
//	@receiver consumer *Consumer
//	@author kevineluo
//	@update 2023-04-18 01:34:40
func (consumer *Consumer) CheckState() {
	var (
		consumerClosed    bool
		readerExist       bool
		workerpoolStopped bool
	)

	select {
	case <-consumer.Closed():
		consumerClosed = true
	default:
	}

	if consumer.reader != nil {
		readerExist = true
	}

	if consumer.workerPool.Stopped() {
		workerpoolStopped = true
	}

	consumer.logger.Info("[Consumer.CheckState] consumer states",
		"consumerClosed", consumerClosed,
		"readerExist", readerExist,
		"workerpoolStopped", workerpoolStopped,
		"runningWorkerNum", consumer.workerPool.RunningWorkers(),
		"idleWorkerNum", consumer.workerPool.IdleWorkers(),
		"consumeErrors", consumer.consumeErrors,
		"deltaOffset", consumer.deltaOffset,
		"topics", consumer.topics)
}

// Close manually close the consumer
//
//	@receiver consumer *Consumer
//	@author kevineluo
//	@update 2023-03-15 01:52:18
func (consumer *Consumer) Close() error {
	select {
	case <-consumer.Closed():
		return ErrClosedConsumer
	default:
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
			if consumer.reader != nil {
				if msg, e := consumer.reader.ReadMessage(consumer.readerCtx); e != nil {
					if e == context.Canceled || e == context.DeadlineExceeded {
						consumer.logger.Info("[Consumer.run] context canceled, restart reading message", "cause", context.Cause(consumer.readerCtx))
					} else if errors.Is(e, io.EOF) {
						// EOF means that the reader has been closed
						consumer.logger.Info("[Consumer.run] reader closed, restart reading message")
					} else {
						consumer.logger.Error(e, "[Consumer.run] error when read message")
						consumer.consumeErrorChan <- e
					}
				} else {
					consumer.logger.Info("[Consumer.run] receive message", "message key", string(msg.Key))
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
					} else {
						consumer.logger.Error(fmt.Errorf("worker pool stopped"), "[Consumer.run] worker pool stopped but still receive message, please report this bug")
					}
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
			consumer.resetReader()
			consumer.noMessageTimer.Reset(consumer.MaxMsgInterval)
		case <-syncTopicTicker.C:
			// tick to sync topics
			syncTopicTicker.Stop()
			if topics, changed, err := consumer.syncTopics(); err != nil {
				consumer.cancel(fmt.Errorf("[Consumer.check] error when check topics: %w", err))
				return
			} else if changed {
				consumer.topics = topics
				// topic change detected, reset kafka reader
				consumer.logger.Info("[Consumer.check] topic change detected, about to reset kafka connection")
				consumer.resetReader()
			}
			syncTopicTicker.Reset(consumer.SyncTopicInterval)
		case err := <-consumer.consumeErrorChan:
			errList = append(errList, err)
			if len(errList) >= consumer.MaxConsumeErrorCount {
				consumer.logger.Error(ErrTooManyConsumeError, "[Consumer.check] too many consume errors", "error list", errList)
				consumer.cancel(ErrTooManyConsumeError)
				return
			}
		}
		consumer.CheckState()
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
	consumer.logger.Info("[Consumer.cleanup] context canceled, about to cleanup resources", "cause", context.Cause(consumer.context), "ID", consumer.id, "delta offset", consumer.deltaOffset)
	if consumer.reader != nil {
		consumer.logger.Info("[Consumer.cleanup] close kafka reader")
		consumer.cancelReader(fmt.Errorf("[Consumer.cleanup] context canceled, about to close kafka reader"))
		err = consumer.reader.Close()
		if err != nil {
			consumer.logger.Error(err, "[Consumer.cleanup] error when close kafka Reader", "cause", context.Cause(consumer.context), "ID", consumer.id, "delta offset", consumer.deltaOffset)
		}
	}
	// wait for workerpool to handle rest messages
	consumer.logger.Info("[Consumer.cleanup] wait for workerpool to handle rest messages")
	consumer.workerPool.StopAndWaitFor(30 * time.Second)

	consumer.logger.Info("[Consumer.cleanup] stop noMessageTimer")
	consumer.noMessageTimer.Stop()

	consumer.logger.Info("[Consumer.cleanup] cleanup finished", "cause", context.Cause(consumer.context), "ID", consumer.id, "delta offset", consumer.deltaOffset)

	return
}

// Closed provide consumer.context.Done() to check if consumer is closed
//
//	@receiver consumer *Consumer
//	@return bool
//	@author kevineluo
//	@update 2023-03-30 05:11:33
func (consumer *Consumer) Closed() <-chan struct{} {
	return consumer.context.Done()
}

// syncTopics syncTopics is used to check if topic list has been changed.
// GetTopics() function is called to get the topics list and then sorted to make sure both lists are in same order. Length of both lists is compared, and if different return true.
// Otherwise loop through each list and compare each element for equality.
//
//	@receiver consumer *Consumer
//	@return changed bool
//	@return err error
//	@author kevineluo
//	@update 2023-02-24 10:31:34
func (consumer *Consumer) syncTopics() (topics []string, changed bool, err error) {
	topics = make([]string, 0)

	if topics, err = consumer.GetTopics(consumer.brokers[0], consumer.Mechanism); err != nil {
		err = fmt.Errorf("[Consumer.CheckTopics] get topics error: %w, GroupID: %s", err, consumer.GroupID)
		return
	}

	consumer.logger.Info("[Consumer.checkTopics] get topics success", "topics", topics)

	if len(topics) != len(consumer.topics) {
		changed = true
		return
	}

	left, right := lo.Difference(consumer.topics, topics)
	changed = len(left) != 0 || len(right) != 0

	return
}

func (consumer *Consumer) resetReader() {
	if consumer.reader != nil {
		// close old reader first, or the new reader will not be able to bind partition(unless waiting for the kafka rebalance)
		consumer.reader.Close()
		consumer.cancelReader(fmt.Errorf("[Consumer.resetReader] about to reset kafka reader"))
	}
	if len(consumer.topics) > 0 {
		readerConfig := kafka.ReaderConfig{
			GroupTopics: consumer.topics,
			GroupID:     consumer.GroupID,
			Brokers:     consumer.brokers,
		}
		if consumer.Verbose {
			readerConfig.Logger = consumer.logger
		}
		consumer.readerCtx, consumer.cancelReader = context.WithCancelCause(consumer.context)
		consumer.reader = kafka.NewReader(readerConfig)
		consumer.logger.Info("[Consumer.resetReader] reset kafka reader success", "topics", consumer.topics)
	} else {
		consumer.logger.Info("[Consumer.resetReader] no topics to reset kafka reader")
	}
}

// GetTopicReMatch function decorator for get topics with regex match, return GetTopicsFunc
// matches found (resTopics) and an err if applicable.
//
//	@param reList []string
//	@return GetTopicsFunc
//	@author kevineluo
//	@update 2023-03-29 03:22:56
func GetTopicReMatch(reList []string) GetTopicsFunc {
	return func(broker string, mechanism sasl.Mechanism) (topics []string, err error) {
		topics = make([]string, 0)
		allTopics, err := getTopics(broker, mechanism)
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
	return func(broker string, mechanism sasl.Mechanism) (topics []string, err error) {
		topics, err = getTopics(broker, mechanism)
		return
	}
}

func getTopics(broker string, mechanism sasl.Mechanism) (topics []string, err error) {
	dialer := kafka.DefaultDialer
	dialer.SASLMechanism = mechanism
	conn, err := dialer.Dial("tcp", broker)
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
