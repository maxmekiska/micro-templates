# Micro-templates
Repository to host micro service implementation patterns. Each implementation should be as simple as possible.

Project created based on: https://stackoverflow.com/questions/75839415/kafka-fastapi-docker-template 

## Call for contributions

Please feel free to contribute to this repository by adding your own implementation of a micro service pattern. The only requirement is that the implementation should be as simple as possible. The goal is to provide a collection of simple implementations of micro service patterns that can be used as a reference for other implementations. Any feedback and contriubtions are welcome and appreciated.



### `Confluent-Kafka-FastAPI-Docker`

#### How to use

1. run `docker-compose up`
2. send example curl request: ```curl -X POST http://localhost:8000/produce -H "Content-Type: application/json" -d '{"key": "nice nice nice"}'```
3. monitor by visting Kafdrop UI at `localhost:9000:9000`

#### Flow

- Producer one with FAST API endpoint to send messages to a Kafka topic: my-topic
- Consumer_Producer service to consume my-topic from Producer one and produce to my-topic-two.
  - Consumer component returns
  - Producer component returns:
- Consumer two service to consume my-topic-two from Consumer_Producer service and print to console