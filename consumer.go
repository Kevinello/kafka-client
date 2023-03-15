package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/alitto/pond"
	"github.com/dlclark/regexp2"
	"github.com/go-logr/logr"
	"github.com/gofrs/uuid"
	"github.com/samber/lo"
	"github.com/segmentio/kafka-go"
	kafkago "github.com/segmentio/kafka-go"
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
type MessageHandler func(msg *kafkago.Message, consumer *Consumer) (err error)

// Consumer struct holds data related to the consumer
//
//	@author kevineluo
//	@update 2023-02-24 01:53:37
type Consumer struct {
	ID         string           // ID of consumer
	Index      int              // Index of message from start offset
	BrokerList []string         // List of brokers that consumer will connect to
	GroupID    string           // Group ID of consumer
	Topics     []string         // Topics to consume
	Reader     *kafkago.Reader  // Reader for consume multiple topics
	WorkerPool *pond.WorkerPool // Pool of worker threads for processing messages

	LastConsumeTime    time.Time     // Last received data, if exceeded:
	MaxMsgInterval     time.Duration // If no message received after [MaxMsgInterval] seconds then restart Consumer
	LastCheckTopicTime time.Time     // Time when topics were last checked
	ErrCount           int           // count errors from consuming message, when it reach
	GetTopics          GetTopicsFunc // Function used to check topics

	close chan error // channel receive close signal and reason(error)
	ready chan bool  // channel used by consumer to signal it is ready

	logger *logger
}

// NewConsumer new Consumer
//
//	@param kafkaBootstrap string
//	@param groupID string
//	@param getTopics GetTopicsFunc
//	@return c *Consumer
//	@return err error
//	@author kevineluo
//	@update 2023-03-14 01:12:16
func NewConsumer(kafkaBootstrap string, groupID string, getTopics GetTopicsFunc, logrLogger logr.Logger) (c *Consumer, err error) {
	topics, err := getTopics()
	if err != nil {
		err = fmt.Errorf("[NewConsumer] getTopics error: %w, GroupID: %s", err, groupID)
		return
	} else if len(topics) == 0 {
		err = fmt.Errorf("[NewConsumer] getTopics error: %w, GroupID: %s", ErrNoTopics, groupID)
		return
	}

	brokers := getBrokerList(kafkaBootstrap)
	logger := &logger{logrLogger}
	reader := kafka.NewReader(kafkago.ReaderConfig{
		GroupTopics: topics,
		GroupID:     groupID,
		Brokers:     brokers,
		Logger:      logger,
	})

	workerPool := pond.New(MaxConsumeGoroutines, 2*MaxConsumeGoroutines, pond.Strategy(pond.Balanced()))

	UUID, _ := uuid.NewV4()
	c = &Consumer{
		ID:                 UUID.String(),
		Index:              0,
		BrokerList:         brokers,
		GroupID:            groupID,
		Topics:             topics,
		Reader:             reader,
		WorkerPool:         workerPool,
		LastConsumeTime:    time.Now(),
		MaxMsgInterval:     time.Duration(ConsumerRestartAfterWaitSec) * time.Second,
		LastCheckTopicTime: time.Now(),
		close:              make(chan error),
		ready:              make(chan bool),
		logger:             logger,
	}

	return
}

// StartConsumeWithHandler start consume messages with exact MessageHandler
//
//	@receiver consumer *Consumer
//	@param ch MessageHandler
//	@return err error
//	@author kevineluo
//	@update 2023-03-14 06:46:28
func (consumer *Consumer) StartConsumeWithHandler(ctx context.Context, messageHandler MessageHandler) (err error) {
	subCtx, cancel := context.WithCancel(ctx)
	defer func() {
		err = consumer.cleanUp(cancel)
	}()

	// goroutine for checking if it has been too long since any data was received.
	go consumer.check(subCtx)

	go func() {
		for {
			// check if consumer.ErrCount reach MaxConsumeErrorCount
			if consumer.ErrCount >= MaxConsumeErrorCount {
				consumer.close <- ErrTooManyConsumeError
				return
			}

			if msg, e := consumer.Reader.ReadMessage(subCtx); e != nil {
				consumer.logger.Error(e, "[Consumer.run] error when read message")
				consumer.ErrCount++
			} else {
				// successful consumption of data
				consumer.WorkerPool.Submit(func() {
					if e = messageHandler(&msg, consumer); e != nil {
						consumer.logger.Error(e, "[Consumer.run] error when handle message")
						consumer.ErrCount++
					} else {
						consumer.Index++
						consumer.LastConsumeTime = time.Now()
					}
				})
			}
		}
	}()

	return
}

// CheckTopics CheckTopics is used to check if topic list has been changed.
// GetTopics() function is called to get the topics list and then sorted to make sure both lists are in same order. Length of both lists is compared, and if different return true.
// Otherwise loop through each list and compare each element for equality.
//
//	@receiver consumer *Consumer
//	@return changed bool
//	@return err error
//	@author kevineluo
//	@update 2023-02-24 10:31:34
func (consumer *Consumer) CheckTopics() (changed bool, err error) {
	newTopics := make([]string, 0)

	if newTopics, err = consumer.GetTopics(); err != nil {
		err = fmt.Errorf("[Consumer.CheckTopics] getTopics error: %w, GroupID: %s", err, consumer.GroupID)
		return
	}

	if len(newTopics) != len(consumer.Topics) {
		changed = true
		return
	}

	left, right := lo.Difference(consumer.Topics, newTopics)
	changed = len(left) != 0 || len(right) != 0

	return
}

// SendCloseSignal etNeedClose sets the needClose field to true and logs the reason for intent to close
//
//	@receiver consumer *Consumer
//	@param reason string
//	@return err error
//	@author kevineluo
//	@update 2023-02-24 11:09:15
func (consumer *Consumer) SendCloseSignal(reason error) (err error) {
	consumer.close <- reason
	return
}

// check check if it has been too long since any data was received / topic change
//
//	@receiver consumer *Consumer pointer to the consumer which calls this function
//	@return err error
//	@author kevineluo
//	@update 2023-02-24 11:33:25
func (consumer *Consumer) check(ctx context.Context) {
	consumer.LastCheckTopicTime = time.Now()

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
		if now.Sub(consumer.LastCheckTopicTime).Seconds() > float64(KafkaCheckTopicPeriodSec) {
			consumer.LastCheckTopicTime = time.Now()

			if len(consumer.Topics) == 0 {
				consumer.SendCloseSignal(ErrNoTopics)
				return
			}

			if changed, err := consumer.CheckTopics(); err != nil {
				consumer.SendCloseSignal(fmt.Errorf("Consumer.CheckTopics error: %w", err))
				return
			} else if changed {
				consumer.SendCloseSignal(ErrTopicChanged)
				return
			}
		}

		// Check if it has been too long since any data was received
		if now.Sub(consumer.LastConsumeTime) > consumer.MaxMsgInterval {
			consumer.SendCloseSignal(ErrTooLongSinceLastConsume)
			return
		}
	}
}

// cleanUp function that closes all opened resources of the Consumer object such as its reader, worker pool
// It returns an error if there were any.
//
//	@receiver consumer *Consumer
//	@return err error
//	@author kevineluo
//	@update 2023-02-24 11:46:25
func (consumer *Consumer) cleanUp(cancel context.CancelFunc) (err error) {
	defer cancel()
	reason := <-consumer.close
	consumer.logger.Info("[Consumer.Close] consumer will be closed.", "reason", reason)
	err = consumer.Reader.Close()
	if err != nil {
		consumer.logger.Error(err, "[Consumer.Close] error when close kafka Reader", "ID", consumer.ID, "index", consumer.Index)
	}
	consumer.WorkerPool.StopAndWait()
	return
}

// GetTopicReMatch this function takes in a list of strings (reList) and returns a list of strings for any
// matches found (resTopics) and an err if applicable.
//
//	@param reList []string
//	@return resTopics []string
//	@return err error
//	@author kevineluo
//	@update 2023-02-24 05:00:53
func GetTopicReMatch(reList []string) (resTopics []string, err error) {
	resTopics = make([]string, 0)
	topics, e := getTopics()
	if e != nil {
		err = errors.New("GetTopicReMatch, getTopics error: " + e.Error())
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

func getTopics() (topics []string, err error) {
	conn, err := kafkago.Dial("tcp", KafkaBootstrap)
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
