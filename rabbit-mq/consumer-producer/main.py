import pika
import json
import time
import os

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq-broker')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', '5672'))

IN_EXCHANGE_NAME = "outbound"
IN_EXCHANGE_TYPE = "topic"
IN_ROUTING_KEY_BINDING = "knowledge.#"

OUT_EXCHANGE_NAME = "final"
OUT_EXCHANGE_TYPE = "topic"
OUT_ROUTING_KEY_PREFIX = "processed."

QUEUE_NAME = "transformer_queue"

INSTANCE_NAME = os.getenv('HOSTNAME', f"local_transformer_{os.getpid()}")

class RabbitMQTransformer:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.connect()

    def connect(self):
        """Establishes a blocking connection to RabbitMQ."""
        while True:
            try:
                connection_params = pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    heartbeat=600, # Set heartbeat for long-lived connections
                    client_properties={'connection_name': f'transformer-{INSTANCE_NAME}'}
                )
                self.connection = pika.BlockingConnection(connection_params)
                self.channel = self.connection.channel()
                print(f"[{INSTANCE_NAME}] Successfully connected to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}")
                break
            except pika.exceptions.AMQPConnectionError as e:
                print(f"[{INSTANCE_NAME}] Failed to connect to RabbitMQ ({e}). Retrying in 5 seconds...")
                time.sleep(5)
            except Exception as e:
                print(f"[{INSTANCE_NAME}] An unexpected error during connection: {e}. Retrying in 5 seconds...")
                time.sleep(5)

    def setup_consumer(self):
        """Declares exchange, queue, and sets up consumption."""
        self.channel.exchange_declare(exchange=IN_EXCHANGE_NAME, exchange_type=IN_EXCHANGE_TYPE, durable=True)
        print(f"[{INSTANCE_NAME}] Declared input exchange '{IN_EXCHANGE_NAME}'.")

        self.channel.queue_declare(queue=QUEUE_NAME, durable=True)
        print(f"[{INSTANCE_NAME}] Declared queue '{QUEUE_NAME}'.")

        self.channel.queue_bind(
            exchange=IN_EXCHANGE_NAME,
            queue=QUEUE_NAME,
            routing_key=IN_ROUTING_KEY_BINDING
        )
        print(f"[{INSTANCE_NAME}] Bound queue '{QUEUE_NAME}' to exchange '{IN_EXCHANGE_NAME}' with routing key '{IN_ROUTING_KEY_BINDING}'.")

        self.channel.basic_qos(prefetch_count=1)

        self.channel.basic_consume(
            queue=QUEUE_NAME,
            on_message_callback=self.on_message_callback,
            auto_ack=False # We will manually acknowledge messages
        )
        print(f"[{INSTANCE_NAME}] Waiting for messages on queue '{QUEUE_NAME}'. To exit, press CTRL+C")

    def setup_producer(self):
        """Declares the output exchange."""
        self.channel.exchange_declare(exchange=OUT_EXCHANGE_NAME, exchange_type=OUT_EXCHANGE_TYPE, durable=True)
        print(f"[{INSTANCE_NAME}] Declared output exchange '{OUT_EXCHANGE_NAME}'.")

    def on_message_callback(self, ch, method, body):
        """Callback function to process incoming messages."""
        try:
            original_payload = json.loads(body.decode('utf-8'))
            print(f"\n[{INSTANCE_NAME}] Received message (delivery_tag: {method.delivery_tag}) from '{method.exchange}' with routing key '{method.routing_key}'.")

            original_payload['processed_by'] = INSTANCE_NAME
            original_payload['processed_timestamp'] = time.time()

            time.sleep(random.uniform(0.5, 2.0)) # Random sleep between 0.5 and 2 seconds

            new_routing_key = f"{OUT_ROUTING_KEY_PREFIX}{method.routing_key}"

            ch.basic_publish(
                exchange=OUT_EXCHANGE_NAME,
                routing_key=new_routing_key,
                body=json.dumps(original_payload),
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE # Make message persistent
                )
            )
            print(f"[{INSTANCE_NAME}] Published modified message to '{OUT_EXCHANGE_NAME}' with routing key '{new_routing_key}'.")

            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[{INSTANCE_NAME}] Acknowledged message delivery_tag: {method.delivery_tag}")

        except json.JSONDecodeError:
            print(f"[{INSTANCE_NAME}] Error: Could not decode JSON from message body. Body: {body.decode('utf-8', errors='ignore')}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False) # Nack and don't requeue bad messages
        except Exception as e:
            print(f"[{INSTANCE_NAME}] Error processing message (delivery_tag: {method.delivery_tag}): {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True) # Requeue on other errors

    def start_consuming(self):
        """Starts the consumer loop."""
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print(f"[{INSTANCE_NAME}] Transformer stopped by user.")
        except pika.exceptions.ConnectionClosedByBroker:
            print(f"[{INSTANCE_NAME}] Connection closed by broker. Reconnecting...")
            self.connect()
            self.setup_consumer()
            self.setup_producer()
            self.start_consuming() # Restart consuming after reconnect
        except pika.exceptions.AMQPConnectionError as e:
            print(f"[{INSTANCE_NAME}] AMQP connection error: {e}. Reconnecting...")
            self.connect()
            self.setup_consumer()
            self.setup_producer()
            self.start_consuming()
        except Exception as e:
            print(f"[{INSTANCE_NAME}] An unexpected error occurred in consuming loop: {e}")
        finally:
            if self.connection:
                print(f"[{INSTANCE_NAME}] Closing RabbitMQ connection.")
                self.connection.close()

if __name__ == "__main__":
    import random # Import here if only used in main scope (e.g. for random sleep)
    transformer = RabbitMQTransformer()
    transformer.setup_consumer()
    transformer.setup_producer()
    transformer.start_consuming()