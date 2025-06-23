import redis
import json
import time
import os
import sys
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

PRODUCER_NAME = os.getenv("HOSTNAME")  # Unique container ID

logging.info(f"I AM PRODUCER: {PRODUCER_NAME}")


def get_redis_connection():
    """Get Redis connection with proper host resolution"""
    # Try to connect to Redis with retry logic
    redis_host = os.getenv("REDIS_HOST", "redis_stream_instance")
    redis_port = int(os.getenv("REDIS_PORT", 6379))

    max_retries = 10
    retry_count = 0

    while retry_count < max_retries:
        try:
            r = redis.Redis(
                host=redis_host,
                port=redis_port,
                decode_responses=True,
                socket_connect_timeout=10,
                socket_timeout=10,
                health_check_interval=30,
            )
            # Test connection
            r.ping()
            logging.info(
                f"Successfully connected to Redis at {redis_host}:{redis_port}"
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


def produce():
    r = get_redis_connection()

    logging.info(f"[{PRODUCER_NAME}] Starting producer...")  # Updated print
    message_count = 0

    try:
        while True:
            message_count += 1
            msg = {
                "user": "max",
                "action": "drop_knowledge",
                "knowledge": {
                    "topic": "redis streams",
                    "depth": "legendary",
                    "timestamp": time.time(),
                    "message_id": message_count,
                    "producer_id": PRODUCER_NAME,  # Add producer ID to the message payload
                },
            }

            stream_id = r.xadd("knowledge_stream", {"data": json.dumps(msg)})
            logging.info(
                f"[{PRODUCER_NAME}] Sent message {message_count} with ID {stream_id}: {msg['knowledge']['topic']}"
            )  # Updated print

            time.sleep(1)

    except KeyboardInterrupt:
        logging.warning(f"[{PRODUCER_NAME}] Producer stopped by user")  # Updated print
    except Exception as e:
        logging.error(f"[{PRODUCER_NAME}] Producer error: {e}")  # Updated print
        sys.exit(1)


if __name__ == "__main__":
    produce()
