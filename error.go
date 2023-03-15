package kafka

import (
	"errors"
)

var (
	// ErrNoTopics GetTopicsFunc got no topic
	//	@update 2023-03-14 01:18:01
	ErrNoTopics = errors.New("no topics found")

	// ErrTopicChanged topic change detected
	//	@update 2023-03-15 02:00:45
	ErrTopicChanged = errors.New("topic change detected")

	// ErrTooManyConsumeError consumer.ErrCount reach MaxConsumeErrorCount
	//	@update 2023-03-15 02:02:27
	ErrTooManyConsumeError = errors.New("too many errors when consuming data")

	// ErrTooLongSinceLastConsume too long since last consume
	//	@update 2023-03-15 02:05:08
	ErrTooLongSinceLastConsume = errors.New("Consumer hasn't get message for too long")

	// ErrClosedConsumer error when try to close closed consumer
	//	@update 2023-03-15 02:03:09
	ErrClosedConsumer = errors.New("the consumer has been closed")

	// ErrInactiveConsumer error when try to close consumer before start it
	//	@update 2023-03-15 02:22:42
	ErrInactiveConsumer = errors.New("the consumer hasn't been started")
)
