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
	"github.com/segmentio/kafka-go"
	. "github.com/smartystreets/goconvey/convey"
)

// TestConsumer test consumer
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
			Bootstrap:      "9.134.95.221:9092",
			GroupID:        "unit-test-group-" + time.Now().Format(time.DateOnly),
			GetTopics:      kc.GetTopicReMatch([]string{"^unit-test-topic-\\d$"}),
			MaxMsgInterval: 30 * time.Second,
			MessageHandler: func(msg *kafka.Message, consumer *kc.Consumer) (err error) {
				defer wg.Done()
				count++
				consumer.Logger.Info("received a message, handle for 1 second", "key", string(msg.Key), "value length", len(msg.Value), "topic", msg.Topic, "offset", msg.Offset, "count", count)
				consumer.CheckState()
				time.Sleep(1 * time.Second)
				return
			},
			MaxConsumeGoroutines: 100,
			// Verbose:              true,
		}
		start := time.Now()
		consumer, err := kc.NewConsumer(context.Background(), config)
		So(err, ShouldBeNil)

		// consume 10000 messages
		wg.Wait()
		consumer.Logger.Info("consume all messages", "count", count, "time", time.Since(start))

		// Close the consumer
		err = consumer.Close()
		So(err, ShouldBeNil)
	})
}

func BenchmarkReset(b *testing.B) {
	testTimer := time.NewTimer(10 * time.Second)
	defer testTimer.Stop()
	var wg sync.WaitGroup
	wg.Add(b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go func() {
			defer wg.Done()
			testTimer.Reset(10 * time.Second)
		}()
	}
	wg.Wait()
}

func BenchmarkTimeNow(b *testing.B) {
	timeNow := time.Now()
	timeNow.Day()
	var wg sync.WaitGroup
	wg.Add(b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go func() {
			defer wg.Done()
			timeNow = time.Now()
		}()
	}
	wg.Wait()
}
