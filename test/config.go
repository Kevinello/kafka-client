package test

import "os"

var (
	kafkaBootstrap string
	saslUsername   string
	saslPassword   string
)

func init() {
	kafkaBootstrap = os.Getenv("KAFKA_BOOTSTRAP")
	if kafkaBootstrap == "" {
		kafkaBootstrap = "localhost:9092"
	}
	saslUsername = os.Getenv("KAFKA_SASL_USERNAME")
	saslPassword = os.Getenv("KAFKA_SASL_PASSWORD")
}
