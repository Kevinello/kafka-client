// Package kafka Manage Kafka Client
//
//	@update 2023-03-28 02:01:25
package test

import (
	"context"
	"strconv"
	"testing"
	"unicode"

	kc "github.com/Kevinello/kafka-client"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/segmentio/kafka-go"
	. "github.com/smartystreets/goconvey/convey"
)

var chineseRunes []rune

func init() {
	for _, range16 := range unicode.ASCII_Hex_Digit.R16 {
		for code := range16.Lo; code <= range16.Hi; code += range16.Stride {
			chineseRunes = append(chineseRunes, rune(code))
		}
	}
	for _, range32 := range unicode.ASCII_Hex_Digit.R32 {
		for code := range32.Lo; code <= range32.Hi; code += range32.Stride {
			chineseRunes = append(chineseRunes, rune(code))
		}
	}
}

// TestProducer test producer
//
//	@param t *testing.T
//	@author kevineluo
//	@update 2023-04-01 11:42:41
func TestProducer(t *testing.T) {
	Convey("Given a kafka producer", t, func() {
		// Create a producer config.
		config := kc.ProducerConfig{
			Bootstrap:              "9.134.95.221:9092",
			AllowAutoTopicCreation: true,
		}
		// New a producer instance.
		producer, err := kc.NewProducer(context.Background(), config)
		So(err, ShouldBeNil)
		Convey("When produce 1000 messages on 10 topics", func() {
			randomString := make([]string, 0)
			for i := 0; i < 10; i++ {
				randomString = append(randomString, lo.RandomString(1000, chineseRunes))
			}
			for i := 0; i < 100; i++ {
				msgs := make([]kafka.Message, 10)
				for j := 0; j < 10; j++ {
					// Generate a random key.
					key := uuid.New().String()
					msgs[j] = kafka.Message{
						Topic: "unit-test-topic-" + strconv.Itoa(j),
						Key:   []byte(key),
						Value: []byte(randomString[j]),
					}
				}
				err = producer.WriteMessages(context.Background(), msgs...)
				So(err, ShouldBeNil)
				producer.Logger.Info("write messages done", "count", len(msgs))
			}
			producer.Logger.Info("write all messages done")

			// Close the producer
			err = producer.Close()
			So(err, ShouldBeNil)
		})
	})
}
