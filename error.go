package kafka

import (
	"errors"
	"fmt"
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
	ErrTooManyConsumeError = fmt.Errorf("There have been %d errors when consuming data", MaxConsumeErrorCount)

	// ErrTooLongSinceLastConsume too long since last consume
	//	@update 2023-03-15 02:05:08
	ErrTooLongSinceLastConsume = fmt.Errorf("Consumer hasn't get message for at least %d seconds", ConsumerRestartAfterWaitSec)
)
