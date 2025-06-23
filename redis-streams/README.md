# Redis Streams

## 1. Download Redis Stack & Set Up Docker Network

```bash
docker pull redis/redis-stack:latest
```

## 2. Create a Docker Network

```bash
docker network create my-redis-network
```

## 3. Run Redis Stack on the Network

```bash
docker run -d --name my-redis-stream --network my-redis-network -p 6379:6379 -p 8001:8001 redis/redis-stack:latest
```

## 4. Build producer

```bash
docker compose up -d --build # Use --scale producer=N if you want more instances than specified in replicas
```

## 5. Build consumer

```bash
docker compose up -d --build --scale consumer=3 # Scales to 3 consumers, overriding replicas if different
```

## 6. Build consumer-producer

```bash
docker compose up -d --build --scale consumer-producer=3 # Scales to 3 consumer-producers, overriding replicas if different
```

```mermaid
graph TD
    subgraph Services
        ProducerService[Producer Service]
        ConsumerService[Consumer Service(Replicas in Group: mygroup)]
        ConsumerProducerService[Consumer-Producer Service(Replicas in Group: mygroup-consumer-producer)]
        RedisDashboard[Redis Dashboard]
    end

    RedisStreams[Redis Streams Instance]

    ProducerService -- produces to knowledge_stream --> RedisStreams
    ConsumerService -- consumes from knowledge_stream --> RedisStreams
    ConsumerProducerService -- consumes from knowledge_stream --> RedisStreams
    ConsumerProducerService -- produces to output_stream --> RedisStreams

    RedisDashboard -- monitors --> RedisStreams
```