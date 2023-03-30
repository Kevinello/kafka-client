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
	"github.com/gofrs/uuid"
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

func TestProducer(t *testing.T) {
	Convey("Given a kafka consumer", t, func() {
		config := kc.ProducerConfig{}
		producer, err := kc.NewProducer(config)
		So(err, ShouldBeNil)
		Convey("When produce 10 messages", func() {
			msgs := make([]kafka.Message, 0)
			for i := 0; i < 10; i++ {
				value := lo.RandomString(1000, chineseRunes)
				msgs = append(msgs, kafka.Message{
					Topic: "unit-test-topic-" + strconv.Itoa(i),
					Key:   lo.Must(uuid.NewV4()).Bytes(),
					Value: []byte(value),
				})
			}
			// TODO: 自动创建topic
			err = producer.WriteMessages(context.Background(), msgs...)
			So(err, ShouldBeNil)
		})
	})
}
