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

func TestConsumer(t *testing.T) {
	Convey("Given a kafka consumer", t, func() {
		var wg sync.WaitGroup
		wg.Add(10)
		config := kc.ConsumerConfig{
			GroupID:        "unit-test-group-" + time.Now().Format(time.DateOnly),
			GetTopics:      kc.GetTopicReMatch([]string{"^unit-test.*$"}),
			MaxMsgInterval: 5 * time.Second,
			MessageHandler: func(msg *kafka.Message, consumer *kc.Consumer) (err error) {
				defer wg.Done()
				consumer.Logger.Info(string(msg.Value), "key", string(msg.Key), "topic", msg.Topic, "offset", msg.Offset)
				return
			},
		}
		consumer, err := kc.NewConsumer(context.Background(), config)
		So(err, ShouldBeNil)

		// consume 10 messages
		wg.Wait()

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
