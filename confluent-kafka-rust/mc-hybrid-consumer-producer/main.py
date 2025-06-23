import json
import logging
from confluent_kafka import Consumer, Producer, KafkaException

from mc_hybrid_consumer_producer import monte_carlo_integrate_rs

KAFKA_BROKER = "kafka:29092"  # 'localhost:9092'
INPUT_TOPIC_NAME = "integration-requests"  # Topic to consume requests from
OUTPUT_TOPIC_NAME = "integration-results"  # Topic to produce results to
CONSUMER_GROUP_ID = "mc-python-processor-group"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def create_consumer():
    """Creates and returns a confluent_kafka.Consumer instance."""
    conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": CONSUMER_GROUP_ID,
        "auto.offset.reset": "latest",  # Start reading from the beginning if no offset is committed
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


def create_producer():
    """Creates and returns a confluent_kafka.Producer instance."""
    conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "acks": "all",  # Ensure all replicas acknowledge the message
        "retries": 3,
        "linger.ms": 100,  # Batch messages for 100ms before sending
    }
    try:
        producer = Producer(conf)
        logging.info(f"Kafka producer connected to '{KAFKA_BROKER}'")
        return producer
    except KafkaException as e:
        logging.error(f"Kafka error creating producer: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error creating producer: {e}")
        return None


def delivery_report(err, msg):
    """
    Callback function to handle delivery reports of produced messages.
    Invoked once the message has been successfully delivered or permanently failed.
    """
    if err is not None:
        logging.error(
            f"Result message delivery failed for key {msg.key().decode('utf-8') if msg.key() else 'None'}: {err}"
        )


def run_processor(consumer, producer):
    """
    Consumes messages, calls Rust for computation, and produces results.
    """
    if not consumer or not producer:
        logging.info("Kafka clients not initialized. Exiting.")
        return

    logging.info(f"Waiting for integration requests from topic '{INPUT_TOPIC_NAME}'...")
    try:
        while True:
            msg = consumer.poll(
                timeout=1.0
            )  # Poll for messages with a 1-second timeout
            if msg is None:
                continue
            if msg.error():
                if msg.error().is_partition_eof():
                    logging.error(
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
                function_id = request_payload.get("function_id")
                dimensions = request_payload.get("dimensions")
                integration_bounds = request_payload.get("integration_bounds")
                num_samples = request_payload.get("num_samples")

                if not all(
                    [
                        task_id,
                        function_id,
                        dimensions,
                        integration_bounds,
                        num_samples is not None,
                    ]
                ):
                    logging.error(
                        f"Malformed request received for key {key}. Skipping: {request_payload}"
                    )
                    continue

                logging.info(
                    f"\nProcessing Task ID: {task_id[:8]}... (Function: '{function_id}', Samples: {num_samples})"
                )

                (
                    estimated_integral,
                    error_estimate,
                    computation_time_ms,
                ) = monte_carlo_integrate_rs(
                    function_id, dimensions, integration_bounds, num_samples
                )

                result_payload = {
                    "task_id": task_id,
                    "estimated_integral": estimated_integral,
                    "error_estimate": error_estimate,
                    "computation_time_ms": computation_time_ms,
                    "processed_by": "python_rust_hybrid",  # For identification
                }

                producer.produce(
                    OUTPUT_TOPIC_NAME,
                    key=task_id.encode("utf-8"),
                    value=json.dumps(result_payload).encode("utf-8"),
                    callback=delivery_report,
                )
                producer.poll(0)  # Non-blocking poll for delivery reports

                logging.info(
                    f"  Result sent for Task ID {task_id[:8]}... Integral: {estimated_integral:.8f}, Time: {computation_time_ms:.2f}ms"
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
        logging.info("\nShutting down processor...")
    finally:
        consumer.close()
        producer.flush(10)  # Ensure all pending results are sent
        producer.close()
        logging.info("Kafka clients closed.")


if __name__ == "__main__":
    consumer_instance = create_consumer()
    producer_instance = create_producer()
    if consumer_instance and producer_instance:
        run_processor(consumer_instance, producer_instance)
