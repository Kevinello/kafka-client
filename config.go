// Package kafka library for kafka-go
//
//	@update 2023-02-23 02:16:52
package kafka

import (
	"strconv"
	"strings"

	"git.woa.com/tencent_cloud_mobile_tools/QAPM_CLOUD/go-between/library/env"
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

	// KafkaMaxThread int Maximum number of threads for subscribing to topics
	//	@update 2023-02-24 10:31:01
	KafkaMaxThread int

	// KafkaCheckTopicPeriodSec int Time period for checking topics
	//	@update 2023-02-24 10:31:08
	KafkaCheckTopicPeriodSec int

	// ConsumerRestartAfterWaitSec int Time period after which consumer will be restarted if no data is received
	//	@update 2023-02-24 10:31:20
	ConsumerRestartAfterWaitSec int
)

func init() {
	KafkaBootstrap = env.GetEnv("APM_KAFKA_BOOTSTRAP", "localhost:9092")
	KafkaConnectHost = env.GetEnv("APM_KAFKA_CONNECT_HOST", "")
	KafkaSchemaRegistryURL = env.GetEnv("APM_KAFKA_SCHEMA_REGISTRY_URL", "")
	KafkaMaxThread, _ = strconv.Atoi(env.GetEnv("KAFKA_MAX_THREAD", "10"))
	KafkaCheckTopicPeriodSec, _ = strconv.Atoi(env.GetEnv("KAFKA_CHECK_TOPIC_PERIOD_SEC", "10"))
	ConsumerRestartAfterWaitSec, _ = strconv.Atoi(env.GetEnv("APM_CONSUMER_RESTART_AFTER_WAIT_SEC", "300"))
}

func getBrokerList(kafkaBootstrap string) (brokerList []string) {
	brokerList = strings.Split(kafkaBootstrap, ",")
	return
}
