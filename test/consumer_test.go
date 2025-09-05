// Package kafka Manage Kafka Client
//
//	@update 2023-03-28 02:01:25
package test

import (
	"context"
	"sync"
	"testing"
	"time"

	kc "github.com/Kevinello/kafka-client"
	"github.com/samber/lo"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	. "github.com/smartystreets/goconvey/convey"
)

// TestConsumer test consumer
// NOTE: should be used with TestProducer(run TestProducer first, then TestConsumer)
//
//	@param t *testing.T
//	@author kevineluo
//	@update 2023-04-01 11:42:04
func TestConsumer(t *testing.T) {
	Convey("Given a kafka consumer", t, func() {
		var wg sync.WaitGroup
		wg.Add(10000)
		count := 0

		config := kc.ConsumerConfig{
			Bootstrap: kafkaBootstrap,
			GroupID:   "unit-test-group-" + time.Now().Format(time.DateOnly),
			GetTopics: kc.GetTopicReMatch([]string{"^unit-test-topic-\\d$"}),
			MessageHandler: func(msg *kafka.Message, consumer *kc.Consumer) (err error) {
				defer wg.Done()
				count++
				consumer.Logger.Info("received a message", "key", string(msg.Key), "value length", len(msg.Value), "topic", msg.Topic, "offset", msg.Offset, "count", count)
				consumer.CheckState()
				return
			},
			MaxConsumeGoroutines: 100,
		}
		if saslUsername != "" && saslPassword != "" {
			mechanism, err := scram.Mechanism(scram.SHA512, saslUsername, saslPassword)
			So(err, ShouldBeNil)
			config.Mechanism = mechanism
		}

		start := time.Now()
		consumer, err := kc.NewConsumer(context.Background(), config)
		So(err, ShouldBeNil)

		// consume 10000 messages
		wg.Wait()
		consumer.Logger.Info("consume all messages", "count", count, "time cost(s)", time.Since(start))

		// Close the consumer
		err = consumer.Close()
		So(err, ShouldBeNil)
	})
}

// BenchmarkConsumer benchmark consumer
// NOTE: Sync(include network cost)		10000	   2122209 ns/op	    1158 B/op	      22 allocs/op
// NOTE: Old Sync(include network cost)	510	   2236091 ns/op	    1128 B/op	      23 allocs/op
//
//	@param b *testing.B
//	@author kevineluo
//	@update 2024-06-20 11:19:56
func BenchmarkConsumer(b *testing.B) {
	ctx := context.Background()
	// Create a consumer
	consumerConfig := kc.ConsumerConfig{
		Bootstrap:   kafkaBootstrap,
		GroupID:     "unit-test-group-" + time.Now().Format(time.DateOnly),
		DisableLoop: true,
		LogLevel:    2, // Error level
		GetTopics:   kc.GetTopicReMatch([]string{"^unit-test-topic$"}),
		MessageHandler: func(msg *kafka.Message, consumer *kc.Consumer) (err error) {
			return
		},
		MaxConsumeGoroutines: 100,
	}
	if saslUsername != "" && saslPassword != "" {
		mechanism, err := scram.Mechanism(scram.SHA512, saslUsername, saslPassword)
		if err != nil {
			b.Fatal(err)
		}
		consumerConfig.Mechanism = mechanism
	}
	consumer, err := kc.NewConsumer(ctx, consumerConfig)
	if err != nil {
		b.Fatal(err)
	}
	defer consumer.Close()

	// Create a producer and write 10000 msgs
	producerConfig := kc.ProducerConfig{
		Bootstrap:              kafkaBootstrap,
		AllowAutoTopicCreation: true,
		LogLevel:               2, // Error level
	}
	if saslUsername != "" && saslPassword != "" {
		mechanism, _ := scram.Mechanism(scram.SHA512, saslUsername, saslPassword)
		producerConfig.Mechanism = mechanism
	}
	producer, err := kc.NewProducer(ctx, producerConfig)
	if err != nil {
		b.Fatal(err)
	}
	randomString := lo.RandomString(100, chineseRunes)
	msg := kafka.Message{
		Topic: "unit-test-topic",
		Key:   []byte(randomString[:10]),
		Value: []byte(randomString),
	}
	msgs := make([]kafka.Message, 10000)
	for i := 0; i < 10000; i++ {
		msgs[i] = msg
	}
	producer.WriteMessages(context.Background(), msgs...)
	producer.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		consumer.ConsumeOnce()
	}
	b.StopTimer()
}
