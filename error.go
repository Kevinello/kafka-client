package kafka

import "errors"

var (

	// ErrNoTopics GetTopicsFunc got no topic
	//	@update 2023-03-14 01:18:01
	ErrNoTopics = errors.New("no topics found")
)
