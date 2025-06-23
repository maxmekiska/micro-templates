import redis
import os
import json
import time
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

logging.info(f"[DEBUG] Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}")


GROUP = "mygroup"

CONSUMER_NAME = os.getenv(
    "HOSTNAME", "unknown_consumer"
)  # Fallback to HOSTNAME which is container ID
COMPOSE_PROJECT_NAME = os.getenv("COMPOSE_PROJECT_NAME", "default_project")
SERVICE_NAME_FROM_COMPOSE = os.getenv(
    "COMPOSE_SERVICE", "consumer"
)  # This is the service name from compose file ('consumer')
CONTAINER_NUMBER_FROM_COMPOSE = os.getenv(
    "COMPOSE_SERVICE_NUMBER", "1"
)  # The instance number (1, 2, 3...)
NAME = f"{SERVICE_NAME_FROM_COMPOSE}-{CONTAINER_NUMBER_FROM_COMPOSE}-{os.getenv('HOSTNAME')[:6]}"  # short HOSTNAME suffix for uniqueness in case of restarts

logging.info(f"I AM CONSUMER: {NAME}")


def consume_group():
    # Ensure the consumer group exists before trying to read from it
    try:
        r.xgroup_create("knowledge_stream", GROUP, mkstream=True)
        logging.info(f"Created consumer group '{GROUP}' for stream 'knowledge_stream'")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            logging.warning(
                f"Consumer group '{GROUP}' already exists for stream 'knowledge_stream'"
            )
        else:
            logging.error(f"Error creating consumer group: {e}")
            return  # Exit if there's an unexpected error

    logging.info(f"Listening as consumer '{NAME}' in group '{GROUP}'...")

    while True:
        messages = r.xreadgroup(
            GROUP, NAME, {"knowledge_stream": ">"}, block=5000, count=1
        )

        if messages:
            stream, entries = messages[0]
            for msg_id, msg_data in entries:
                try:
                    payload = json.loads(msg_data["data"])
                    logging.info(f"\nIAM{NAME} Received [{msg_id}] from stream:")
                    logging.info(json.dumps(payload, indent=2))

                    # simulate processing
                    time.sleep(3)

                    # ACK that we processed it
                    r.xack("knowledge_stream", GROUP, msg_id)
                    logging.info(f"ACKed message: {msg_id}")
                except Exception as e:
                    logging.error(f"Error processing message {msg_id}: {e}")
        else:
            pass  # Keep waiting for messages


if __name__ == "__main__":
    consume_group()
