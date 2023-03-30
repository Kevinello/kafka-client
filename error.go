package kafka

import (
	"errors"
)

var (
	// ErrNoTopics GetTopicsFunc got no topic
	//	@update 2023-03-14 01:18:01
	ErrNoTopics = errors.New("no topics found")

	// ErrTooManyConsumeError consumer.ErrCount reach MaxConsumeErrorCount
	//	@update 2023-03-15 02:02:27
	ErrTooManyConsumeError = errors.New("too many errors when consuming data")

	// ErrClosedConsumer error when try to close closed consumer
	//	@update 2023-03-15 02:03:09
	ErrClosedConsumer = errors.New("the consumer has been closed")

	// ErrClosedProducer error when try to close closed producer
	//	@update 2023-03-30 05:12:28
	ErrClosedProducer = errors.New("the producer has been closed")
)
