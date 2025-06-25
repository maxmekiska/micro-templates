import pika
import json
import time
import os
import sys

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
EXCHANGE_NAME = "outbound"
EXCHANGE_TYPE = "topic"
ROUTING_KEY_PREFIX = "knowledge.new" # Example routing key prefix

PRODUCER_NAME = os.getenv('HOSTNAME', f"local_producer_{os.getpid()}")

def get_rabbitmq_connection():
    """Establishes a blocking connection to RabbitMQ."""
    try:
        connection_params = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            client_properties={'connection_name': f'producer-{PRODUCER_NAME}'}
        )
        connection = pika.BlockingConnection(connection_params)
        print(f"[{PRODUCER_NAME}] Successfully connected to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}")
        return connection
    except pika.exceptions.AMQPConnectionError as e:
        print(f"[{PRODUCER_NAME}] Failed to connect to RabbitMQ: {e}")
        sys.exit(1)

def produce_messages():
    connection = None
    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()

        channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE, durable=True)
        print(f"[{PRODUCER_NAME}] Exchange '{EXCHANGE_NAME}' ({EXCHANGE_TYPE}) declared.")

        message_count = 0
        while True:
            message_count += 1
            current_routing_key = f"{ROUTING_KEY_PREFIX}.{time.strftime('%Y%m%d')}"

            message_payload = {
                "source": PRODUCER_NAME,
                "event_id": message_count,
                "timestamp": time.time(),
                "data": {
                    "topic_area": "distributed_systems",
                    "content": f"Message {message_count} from producer {PRODUCER_NAME}",
                    "urgency": "high" if message_count % 5 == 0 else "low"
                }
            }

            channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=current_routing_key,
                body=json.dumps(message_payload),
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE # Make message persistent
                )
            )
            print(f"[{PRODUCER_NAME}] Sent message {message_count} to '{EXCHANGE_NAME}' with routing key '{current_routing_key}'.")

            time.sleep(1) # Send one message per second

    except KeyboardInterrupt:
        print(f"[{PRODUCER_NAME}] Producer stopped by user.")
    except Exception as e:
        print(f"[{PRODUCER_NAME}] An error occurred: {e}")
    finally:
        if connection:
            print(f"[{PRODUCER_NAME}] Closing RabbitMQ connection.")
            connection.close()

if __name__ == "__main__":
    produce_messages()