import json
from confluent_kafka import Consumer, KafkaException, KafkaError
import logging


KAFKA_BROKER = "kafka:29092"  # 'localhost:9092'
INPUT_TOPIC_NAME = "integration-results"  # Topic to consume requests from
CONSUMER_GROUP_ID = "mc-python-processor-group"


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def create_consumer():
    """Creates and returns a confluent_kafka.Consumer instance."""
    conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": CONSUMER_GROUP_ID,
        "auto.offset.reset": "latest",  # Start reading from the latest offset if no offset is committed
        "enable.auto.commit": True,  # Automatically commit offsets
        "auto.commit.interval.ms": 5000,  # Commit every 5 seconds
    }
    try:
        consumer = Consumer(conf)
        logging.info(
            f"Kafka consumer connected to '{KAFKA_BROKER}' with group '{CONSUMER_GROUP_ID}'"
        )
        consumer.subscribe([INPUT_TOPIC_NAME])
        logging.info(f"Subscribed to topic '{INPUT_TOPIC_NAME}'")
        return consumer
    except KafkaException as e:
        logging.error(f"Kafka error creating consumer: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error creating consumer: {e}")
        return None


def run_processor(consumer):
    """
    Consumes messages, calls Rust for computation, and produces results.
    """
    if not consumer:
        logging.info("Kafka clients not initialized. Exiting.")
        return

    logging.info(
        f"Waiting for integration result requests from topic '{INPUT_TOPIC_NAME}'..."
    )
    try:
        while True:
            msg = consumer.poll(
                timeout=1.0
            )  # Poll for messages with a 1-second timeout
            if msg is None:
                continue
            if msg.error():
                # Corrected way to check for end-of-partition (EOF)
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.info( # Changed to INFO as EOF is not an error but a boundary
                        f"Reached end of partition {msg.partition()} for topic {msg.topic()}"
                    )
                else:
                    logging.error(f"Consumer error: {msg.error()}")
                continue

            try:
                key = msg.key().decode("utf-8") if msg.key() else "None"
                value = msg.value().decode("utf-8")
                request_payload = json.loads(value)

                task_id = request_payload.get("task_id")
                estimated_integral = request_payload.get("estimated_integral")
                error_estimate = request_payload.get("error_estimate")
                computation_time_ms = request_payload.get("computation_time_ms")
                processed_by = request_payload.get("processed_by")

                if not all(
                    [
                        task_id,
                        estimated_integral,
                        error_estimate,
                        computation_time_ms,
                        processed_by,
                    ]
                ):
                    logging.error(
                        f"Malformed request received for key {key}. Skipping: {request_payload}"
                    )
                    continue

                logging.info(
                    f"\nProcessing Task ID: {task_id[:8]}... (Processed by: {processed_by}, duration: {computation_time_ms} ms)"
                )

            except json.JSONDecodeError as e:
                logging.error(
                    f"Failed to decode JSON from message (key: {key}): {e}, Raw: {value}"
                )
            except UnicodeDecodeError as e:
                logging.error(f"Failed to decode message value (key: {key}): {e}")
            except KeyError as e:
                logging.error(
                    f"Missing expected key in message payload (key: {key}): {e}"
                )
            except Exception as e:
                logging.error(
                    f"An unexpected error occurred during processing for key {key}: {e}"
                )

    except KeyboardInterrupt:
        logging.info("\nShutting down processor...") # Changed to INFO
    finally:
        consumer.close()
        logging.info("Kafka clients closed.")


if __name__ == "__main__":
    consumer_instance = create_consumer()
    if consumer_instance:
        run_processor(consumer_instance)

