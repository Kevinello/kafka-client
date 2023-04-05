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

### Kafka producer

- Asynchronously / Synchronously write message
- Safe manual close
- Asynchronous cancellations and timeouts using Go contexts
- Custom logger and verbose option

## Project Structure

![project-structure](https://raw.githubusercontent.com/Kevinello/kafka-client/diagram/images/project-structure.svg)
