# integration_request_generator.py
import json
import time
import uuid
import random
import logging
from confluent_kafka import Producer, KafkaException

KAFKA_BROKER = "kafka:29092"  #'localhost:9092'
INPUT_TOPIC_NAME = "integration-requests"  # Topic to produce integration requests to
OUTPUT_TOPIC_NAME = "integration-results"  # Topic where results will be published by the Python processor

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def delivery_report(err, msg):
    """
    Callback function to handle delivery reports of produced messages.
    Invoked once the message has been successfully delivered or permanently failed.
    """
    if err is not None:
        logging.error(
            f"Message delivery failed for key {msg.key().decode('utf-8') if msg.key() else 'None'}: {err}"
        )
    else:
        logging.error(
            f"Message delivered to topic '{msg.topic()}' [{msg.partition()}] @ offset {msg.offset()} "
            f"for key {msg.key().decode('utf-8') if msg.key() else 'None'}"
        )


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
        logging.error(f"Kafka error connecting to producer: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error creating producer: {e}")
        return None


def produce_integration_requests(producer):
    """Sends Monte Carlo integration requests to the Kafka topic."""
    if not producer:
        logging.info("Producer not initialized. Exiting.")
        return

    request_count = 0
    print(f"Starting to produce integration requests to topic '{INPUT_TOPIC_NAME}'...")

    functions_to_integrate = [
        {
            "function_id": "gaussian_2d",
            "dimensions": 2,
            "integration_bounds": [(-2.0, 2.0), (-2.0, 2.0)],
            "num_samples": 50_000,  # _000 # 50 million samples for a heavy computation
        },
        {
            "function_id": "sin_cos_poly_3d",
            "dimensions": 3,
            "integration_bounds": [
                (0.0, 3.141592653589793),
                (0.0, 3.141592653589793),
                (0.0, 1.0),
            ],
            "num_samples": 100_000,  # _000 # 100 million samples, even heavier
        },
        {
            "function_id": "gaussian_2d",
            "dimensions": 2,
            "integration_bounds": [(-1.0, 1.0), (-1.0, 1.0)],
            "num_samples": 10_000,  # _000 # 10 million samples, lighter
        },
    ]

    while True:
        time.sleep(1)  # Sleep to avoid overwhelming the producer
        config = random.choice(functions_to_integrate)

        task_id = str(uuid.uuid4())  # Generate a unique task ID
        msg_payload = {
            "task_id": task_id,
            "function_id": config["function_id"],
            "dimensions": config["dimensions"],
            "integration_bounds": config["integration_bounds"],
            "num_samples": config["num_samples"],
        }

        message_key = task_id

        try:
            producer.produce(
                INPUT_TOPIC_NAME,
                key=message_key.encode("utf-8"),
                value=json.dumps(msg_payload).encode("utf-8"),
                callback=delivery_report,
            )

            producer.poll(0)

            request_count += 1
            logging.info(
                f"Sent request #{request_count} (Task ID: {task_id[:8]}...) for function '{config['function_id']}' with {config['num_samples']} samples."
            )

        except BufferError:
            logging.error("Producer queue full. Flushing to free up space...")
            producer.flush(10)  # Flush with a timeout of 10 seconds
            producer.produce(
                INPUT_TOPIC_NAME,
                key=message_key.encode("utf-8"),
                value=json.dumps(msg_payload).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)  # Poll again after retry
        except KafkaException as e:
            logging.error(f"Error producing message: {e}")
            # Consider more robust error handling here
        except KeyboardInterrupt:
            logging.info("\nShutting down producer...")
            break  # Exit the loop on Ctrl+C


if __name__ == "__main__":
    producer_instance = create_producer()
    if producer_instance:
        try:
            produce_integration_requests(producer_instance)
        finally:
            logging.info("\nFlushing any pending producer messages before closing...")
            producer_instance.flush(10)  # Ensure all queued messages are sent
            producer_instance.close()
            logging.info("Producer closed.")
