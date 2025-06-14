# `Confluent-Kafka-FastAPI-Docker` [deprecated]

Initial implementation of a micro service pattern using FastAPI and Confluent Kafka. This implementation is deprecated and is not recommended for use. Please refer to the `Confluent-Kafka-Rust` implementation for a more robust solution.

## How to use

1. run `docker-compose up`
2. send example curl request: ```curl -X POST http://localhost:8000/produce -H "Content-Type: application/json" -d '{"key": "nice nice nice"}'```
3. monitor by visting Kafdrop UI at `localhost:9000:9000`

## Flow

- Producer one with FAST API endpoint to send messages to a Kafka topic: my-topic
- Consumer-Producer service to consume my-topic from Producer one and produce to my-topic-two.
  - Consumer component returns
  - Producer component returns
- Consumer two service to consume my-topic-two from Consumer-Producer service and print to console