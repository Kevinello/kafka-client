// Package kafka library for kafka-go
//
//	@update 2023-02-23 02:16:52
package kafka

import (
	"strconv"
	"strings"
)

var (

	// KafkaBootstrap string Address of the Kafka Bootstrap server
	//	@update 2023-02-24 10:30:28
	KafkaBootstrap string

	// KafkaConnectHost string Hostname for connecting to Kafka
	//	@update 2023-02-24 10:30:35
	KafkaConnectHost string

	// KafkaSchemaRegistryURL string URL for schema registry service
	//	@update 2023-02-24 10:30:47
	KafkaSchemaRegistryURL string

	// MaxConsumeGoroutines int Maximum number of goroutine for subscribing to topics
	//	@update 2023-02-24 10:31:01
	MaxConsumeGoroutines int

	// KafkaCheckTopicPeriodSec int Time period for checking topics
	//	@update 2023-02-24 10:31:08
	KafkaCheckTopicPeriodSec int

	// MaxConsumeErrorCount int max error count for consuming messages
	//	@update 2023-03-15 01:15:35
	MaxConsumeErrorCount int

	// ConsumerRestartAfterWaitSec int Time period after which consumer will be restarted if no data is received
	//	@update 2023-02-24 10:31:20
	ConsumerRestartAfterWaitSec int
)

func init() {
	KafkaBootstrap = GetEnv("APM_KAFKA_BOOTSTRAP", "localhost:9092")
	KafkaConnectHost = GetEnv("APM_KAFKA_CONNECT_HOST", "")
	KafkaSchemaRegistryURL = GetEnv("APM_KAFKA_SCHEMA_REGISTRY_URL", "")
	MaxConsumeGoroutines, _ = strconv.Atoi(GetEnv("KAFKA_MAX_THREAD", "10"))
	KafkaCheckTopicPeriodSec, _ = strconv.Atoi(GetEnv("KAFKA_CHECK_TOPIC_PERIOD_SEC", "10"))
	MaxConsumeErrorCount, _ = strconv.Atoi(GetEnv("KAFKA_MAX_ERROR_COUNT", "5"))
	ConsumerRestartAfterWaitSec, _ = strconv.Atoi(GetEnv("APM_CONSUMER_RESTART_AFTER_WAIT_SEC", "300"))
}

func getBrokerList(kafkaBootstrap string) (brokerList []string) {
	brokerList = strings.Split(kafkaBootstrap, ",")
	return
}
