package kafka

import (
	"errors"
	"fmt"
	"time"

	"git.woa.com/tencent_cloud_mobile_tools/QAPM_CLOUD/go-between/library/collections"
	"git.woa.com/tencent_cloud_mobile_tools/QAPM_CLOUD/go-between/library/workerpool"
	"git.woa.com/tencent_cloud_mobile_tools/QAPM_CLOUD/go-between/library/zaplog"
	"github.com/dlclark/regexp2"
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
	ID            string                 // ID of consumer
	Index         int                    // Index of message from start offset
	BrokerList    []string               // List of brokers that consumer will connect to
	GroupID       string                 // Group ID of consumer
	Topics        []string               // Topics to consume
	ConsumerGroup *kafkago.ConsumerGroup // Consumer Group for consume multiple topics
	WorkerPool    *workerpool.WorkerPool // Pool of worker threads for processing messages

	LastConsumeTime    time.Time     // Last received data, if exceeded:
	MaxMsgInterval     time.Duration // If no message received after [MaxMsgInterval] seconds then restart Consumer
	LastCheckTopicTime time.Time     // Time when topics were last checked
	GetTopics          GetTopicsFunc // Function used to check topics

	close chan bool // Set this variable to true, when Topic changes, so that Consumer will be restarted
	ready chan bool // Channel used by consumer to signal it is ready
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
func NewConsumer(kafkaBootstrap string, groupID string, getTopics GetTopicsFunc) (c *Consumer, err error) {
	zaplog.Logger.Info("[NewConsumer]", zaplog.String("kafkaBootstrap", kafkaBootstrap), zaplog.String("groupID", groupID))
	var topics []string
	topics, err = getTopics()
	if err != nil {
		err = fmt.Errorf("[NewConsumer] getTopics error: %w, GroupID: %s", err, groupID)
		return
	} else if len(topics) == 0 {
		err = fmt.Errorf("[NewConsumer] getTopics error: %w, GroupID: %s", ErrNoTopics, groupID)
		return
	}
	zaplog.Logger.Info("[NewConsumer] getTopics success", zaplog.Strings("TopicList", topics))

	config := kafkago.ConsumerGroupConfig{
		Brokers:     []string{kafkaBootstrap},
		Topics:      topics,
		ID:          groupID,
		Logger:      zaplog.Logger,
		StartOffset: kafkago.FirstOffset,
	}

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

	changed = !collections.SliceEqual(consumer.Topics, newTopics)

	return
}

// SetNeedClose etNeedClose sets the needClose field to true and logs the reason for intent to close
//
//	@receiver consumer *Consumer
//	@param reason string
//	@return err error
//	@author kevineluo
//	@update 2023-02-24 11:09:15
func (consumer *Consumer) SetNeedClose(reason string) (err error) {
	zaplog.Logger.Info("[Consumer.SetNeedClose] will close the consumer and consumer.needClose will be set to True", zaplog.String("reason", reason))
	consumer.close <- true
	return
}

// Check checks if it has been too long since any data was received.
//
//	@receiver consumer *Consumer pointer to the consumer which calls this function
//	@return err error
//	@author kevineluo
//	@update 2023-02-24 11:33:25
func (consumer *Consumer) Check() (err error) {
	consumer.LastCheckTopicTime = time.Now()

	for {
		now := time.Now()
		if now.Sub(consumer.LastCheckTopicTime).Seconds() > float64(KafkaCheckTopicPeriodSec) {
			consumer.LastCheckTopicTime = time.Now()

			if len(consumer.Topics) == 0 {
				consumer.SetNeedClose("topic list is empty")
				return
			}

			var changed bool
			if changed, err = consumer.CheckTopics(); err != nil {
				consumer.SetNeedClose(fmt.Sprintf("Consumer.CheckTopics error: %s", err.Error()))
				return
			}
			if changed {
				consumer.SetNeedClose("check topic changed")
				return
			}
		}

		// Check if it has been too long since any data was received
		if now.Sub(consumer.LastConsumeTime) > consumer.MaxMsgInterval {
			consumer.SetNeedClose(fmt.Sprintf("consumer hasn't get message for %s", now.Sub(consumer.LastConsumeTime).String()))
			return
		}

		time.Sleep(time.Second)
	}
}

// Close function that closes all opened resources of the Consumer object such as its reader.
// It returns an error if there were any.
//
//	@receiver consumer *Consumer
//	@return err error
//	@author kevineluo
//	@update 2023-02-24 11:46:25
func (consumer *Consumer) Close() (err error) {
	err = consumer.ConsumerGroup.Close()
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
	zaplog.Logger.Info("[GetTopicReMatch] success", zaplog.Strings("reList", reList), zaplog.Strings("resTopics", resTopics))
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
	topics = collections.Keys(m)
	return
}
