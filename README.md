# kafka-client

kafka-client is a high level wrapped kafka API for kafka consuming and producing based on [kafka-go](https://github.com/segmentio/kafka-go)

## Features

### Kafka consumer

- Customized Message Handler
- Multi topics Consumption(customized get topic function)
- Concurrent workerpool for consuming messages(flexible workerpool size)
- Consume error collection(when there is too many error, consumer will cancel itself)
- Consumption frequency detect(reconnect kafka when message frequency is too low)
- Safe manual close
- Asynchronous cancellations and timeouts using Go contexts
- Custom logger and verbose option
- SASL authentication
- TLS authentication

### Kafka producer

- Asynchronously / Synchronously write message
- Safe manual close
- Asynchronous cancellations and timeouts using Go contexts
- Custom logger and verbose option
- SASL authentication
- TLS authentication

## Usage

For basic usage examples, refer to the system test cases in the `test` directory.

To run the system tests, you need to have a Kafka broker(set [auto.create.topics.enable](https://kafka.apache.org/documentation/#brokerconfigs_auto.create.topics.enable) to `true`, or create topics `unit-test-topic-1` ... `unit-test-topic-10` before starting tests) and set environment variables below:

- KAFKA_BOOTSTRAP: kafka broker address
- KAFKA_SASL_USERNAME(optional): kafka sasl username
- KAFKA_SASL_PASSWORD(optional): kafka sasl password

## SASL & TLS

To use SASL or TLS, you need to set Mechanism or TLS in `ConsumerConfig` and `ProducerConfig`, use `ConsumerConfig` as an example:

```go
package test

import (
	"context"
	"crypto/tls"
	"log"
	"os"

	kc "github.com/Kevinello/kafka-client"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

func main() {
	mechanism, _ := scram.Mechanism(scram.SHA512, "username", "password")
	config := kc.ConsumerConfig{
		GroupID: "test-group",
		MessageHandler: func(msg *kafka.Message, consumer *kc.Consumer) (err error) {
			log.Printf("message: %s\n", string(msg.Value))
			return
		},
		Mechanism: mechanism,
		TLS:       &tls.Config{},
	}
	consumer, _ := kc.NewConsumer(context.Background(), config)
	select {
	case <-consumer.Closed():
		os.Exit(1)
	}
}
```

Replace the `username` and `password` with your own, and fill the `TLS` with your own tls config.

## Project Structure

![project-structure](https://raw.githubusercontent.com/Kevinello/kafka-client/diagram/images/project-structure.svg)
