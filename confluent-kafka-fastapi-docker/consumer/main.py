from confluent_kafka import Consumer, KafkaError
import time 

import logging
logging.basicConfig(level=logging.DEBUG)


conf = {
    'bootstrap.servers': 'kafka:29092',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'group.id': 'my-group',
    'api.version.request': True,
    'api.version.fallback.ms': 0
}

def consume_messages():
    consumer = Consumer(conf)

    consumer.subscribe(['my-topic-two'])

    try:
        while True:
            msg = consumer.poll(1.0)
            logging.info("Consumer two Polling")
            logging.info(msg)

            if msg is None:
                logging.info("No message for consumer two")
                continue

            if msg.error():
                logging.info("Error")
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f'Reached end of partition: {msg.topic()}[{msg.partition()}]')
                else:
                    print(f'Error while consuming messages: {msg.error()}')
                    logging.info(msg.error())
            else:
                print(f"Received message on consumer two: {msg.value().decode('utf-8')}")
                logging.info(msg.value().decode('utf-8'))

    except Exception as e:
        print(f"Exception occurred while consuming messages: {e}")
        logging.info(e)
    finally:
        consumer.close()
        logging.info("Consumer two closed")


def startup():
    logging.info("Starting consumer two...")
    time.sleep(30)
    consume_messages()

if __name__ == "__main__":
    try:
        startup()
    except Exception as e:
        print(f"Exception occurred: {e}")
