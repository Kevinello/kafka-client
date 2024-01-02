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
	"github.com/samber/lo"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
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
			Bootstrap:              kafkaBootstrap,
			AllowAutoTopicCreation: true,
		}
		if saslUsername != "" && saslPassword != "" {
			mechanism, err := scram.Mechanism(scram.SHA512, saslUsername, saslPassword)
			So(err, ShouldBeNil)
			config.Mechanism = mechanism
		}

		// New a producer instance.
		producer, err := kc.NewProducer(context.Background(), config)
		So(err, ShouldBeNil)
		Convey("When produce 10000 messages on 10 topics", func() {
			randomString := make([]string, 0)
			for i := 0; i < 1000; i++ {
				randomString = append(randomString, lo.RandomString(100, chineseRunes))
			}
			for i := 0; i < 10; i++ {
				topic := "unit-test-topic-" + strconv.Itoa(i)
				msgs := make([]kafka.Message, 1000)
				// prepare messages
				for j := 0; j < 1000; j++ {
					msgs[j] = kafka.Message{
						Topic: topic,
						Key:   []byte(randomString[j][:10]),
						Value: []byte(randomString[j]),
					}
				}
				// write messages
				err = producer.WriteMessages(context.Background(), msgs...)
				So(err, ShouldBeNil)
				producer.Logger.Info("write messages done", "topic", topic, "count", len(msgs))
			}
			producer.Logger.Info("write all messages done")

			// Close the producer
			err = producer.Close()
			So(err, ShouldBeNil)
		})
	})
}
