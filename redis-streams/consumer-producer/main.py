import redis
import json
import time
import os
import sys
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

REDIS_HOST = os.getenv(
    "REDIS_HOST", "redis_stream_instance"
)  # Default aligned with docker-compose intent
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

INPUT_STREAM_NAME = "knowledge_stream"
OUTPUT_STREAM_NAME = "output_stream"

CONSUMER_GROUP_NAME = "mygroup-consumer-producer"

COMPOSE_PROJECT_NAME = os.getenv("COMPOSE_PROJECT_NAME", "default_project")
SERVICE_NAME_FROM_COMPOSE = os.getenv("COMPOSE_SERVICE", "consumer-producer")
CONTAINER_NUMBER_FROM_COMPOSE = os.getenv("COMPOSE_SERVICE_NUMBER", "1")
INSTANCE_ID = f"{SERVICE_NAME_FROM_COMPOSE}-{CONTAINER_NUMBER_FROM_COMPOSE}-{os.getenv('HOSTNAME')[:6]}"

logging.info(f"Initializing consumer-producer instance: {INSTANCE_ID}")
logging.info(f"[DEBUG] Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}")


def get_redis_connection() -> redis.Redis:
    """Get Redis connection with retry logic."""
    max_retries = 10
    retry_count = 0

    while retry_count < max_retries:
        try:
            r = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                decode_responses=True,
                socket_connect_timeout=10,
                socket_timeout=10,
                health_check_interval=30,
            )
            # Test connection
            r.ping()
            logging.info(
                f"Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}"
            )
            return r
        except redis.ConnectionError as e:
            retry_count += 1
            logging.error(
                f"Failed to connect to Redis (attempt {retry_count}/{max_retries}): {e}"
            )
            if retry_count < max_retries:
                time.sleep(2)
            else:
                logging.warning("Max retries reached. Exiting.")
                sys.exit(1)


def produce(msg: dict, conn: redis.Redis) -> None:
    """
    Publishes a message to the output stream.
    """
    try:
        stream_id = conn.xadd(OUTPUT_STREAM_NAME, {"data": json.dumps(msg)})
        logging.info(
            f"[{INSTANCE_ID}] Produced message with ID {stream_id} to '{OUTPUT_STREAM_NAME}'"
        )
    except Exception as e:
        logging.error(f"[{INSTANCE_ID}] Producer error: {e}")


def consume_and_produce_stream(conn: redis.Redis) -> None:
    """
    Consumes messages from an input stream using a consumer group
    and produces processed messages to an output stream.
    """
    try:
        conn.xgroup_create(INPUT_STREAM_NAME, CONSUMER_GROUP_NAME, mkstream=True)
        logging.info(
            f"Created consumer group '{CONSUMER_GROUP_NAME}' for stream '{INPUT_STREAM_NAME}'"
        )
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            logging.warning(
                f"Consumer group '{CONSUMER_GROUP_NAME}' already exists for stream '{INPUT_STREAM_NAME}'"
            )
        else:
            logging.error(f"Error creating consumer group: {e}")
            sys.exit(1)  # Exit if there's an unexpected error with group creation

    logging.info(
        f"Listening as consumer '{INSTANCE_ID}' in group '{CONSUMER_GROUP_NAME}' on stream '{INPUT_STREAM_NAME}'..."
    )

    while True:
        try:
            messages = conn.xreadgroup(
                CONSUMER_GROUP_NAME,
                INSTANCE_ID,
                {INPUT_STREAM_NAME: ">"},
                block=5000,
                count=1,
            )

            if messages:
                stream_name, entries = messages[
                    0
                ]  # messages is a list of (stream_name, [entries])
                for msg_id, msg_data in entries:
                    try:
                        payload = json.loads(msg_data["data"])
                        logging.info(
                            f"\n[{INSTANCE_ID}] Received [{msg_id}] from '{stream_name}': {payload}"
                        )

                        payload["processed_by"] = INSTANCE_ID
                        payload["processing_timestamp"] = time.time()
                        logging.info(f"Processing message: {payload}")
                        time.sleep(1)  # Simulate work

                        produce(msg=payload, conn=conn)

                        conn.xack(INPUT_STREAM_NAME, CONSUMER_GROUP_NAME, msg_id)
                        logging.info(f"ACKed message: {msg_id}")

                    except json.JSONDecodeError as e:
                        logging.error(
                            f"Error decoding JSON payload for message {msg_id}: {e}"
                        )
                        conn.xack(
                            INPUT_STREAM_NAME, CONSUMER_GROUP_NAME, msg_id
                        )  # ACK to avoid re-processing bad data

                    except Exception as e:
                        logging.error(
                            f"Unhandled error processing message {msg_id}: {e}"
                        )
            else:
                # No messages received within the block timeout
                logging.debug(f"[{INSTANCE_ID}] No new messages, waiting...")

        except redis.exceptions.ConnectionError as e:
            logging.error(f"Redis connection lost: {e}. Attempting to reconnect...")
            conn = get_redis_connection()  # Attempt to re-establish connection
            if conn is None:  # If reconnection fails
                logging.error("Failed to re-establish Redis connection. Exiting.")
                sys.exit(1)
            time.sleep(2)  # Short delay before next read attempt

        except KeyboardInterrupt:
            logging.warning(f"[{INSTANCE_ID}] Consumer-producer stopped by user.")
            break  # Exit the loop on Ctrl+C
        except Exception as e:
            logging.error(
                f"[{INSTANCE_ID}] An unexpected error occurred in main loop: {e}"
            )
            time.sleep(5)  # Prevent tight loop on recurring errors


if __name__ == "__main__":
    redis_conn = get_redis_connection()
    consume_and_produce_stream(conn=redis_conn)
