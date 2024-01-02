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

### Kafka producer

- Asynchronously / Synchronously write message
- Safe manual close
- Asynchronous cancellations and timeouts using Go contexts
- Custom logger and verbose option
- SASL authentication

## Usage

For basic usage examples, refer to the system test cases in the `test` directory.

To run the system tests, you need to have a Kafka broker(set [auto.create.topics.enable](https://kafka.apache.org/documentation/#brokerconfigs_auto.create.topics.enable) to `true`, or create topics `unit-test-topic-1` ... `unit-test-topic-10` before starting tests) and set environment variables below:

- KAFKA_BOOTSTRAP: kafka broker address
- KAFKA_SASL_USERNAME(optional): kafka sasl username
- KAFKA_SASL_PASSWORD(optional): kafka sasl password

## Project Structure

![project-structure](https://raw.githubusercontent.com/Kevinello/kafka-client/diagram/images/project-structure.svg)
