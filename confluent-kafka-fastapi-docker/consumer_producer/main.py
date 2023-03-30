from confluent_kafka import Consumer, KafkaError, Producer
import time 
import json

import logging
logging.basicConfig(level=logging.DEBUG)



producer_conf = {
    'bootstrap.servers': 'kafka:29092',
    'client.id': 'my-app'
}

producer = Producer(producer_conf)

def produce(data: dict):
    try:
        data = json.dumps(data).encode('utf-8')
        producer.produce('my-topic-two', value=data)
        producer.flush()
        return {"message": "even more messages", "passed_in": data, "status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


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

    consumer.subscribe(['my-topic'])

    try:
        while True:
            msg = consumer.poll(1.0)
            logging.info("Consumer one Polling")
            logging.info(msg)

            if msg is None:
                logging.info("No message for consumer one")
                continue

            if msg.error():
                logging.info("Error")
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f'Reached end of partition: {msg.topic()}[{msg.partition()}]')
                else:
                    print(f'Error while consuming messages: {msg.error()}')
                    logging.info(msg.error())
            else:
                print(f"Received message on consumer one: {msg.value().decode('utf-8')}")
                output = produce({"message": msg.value().decode('utf-8')})
                logging.info(msg.value().decode('utf-8'))
                logging.info(output)

    except Exception as e:
        print(f"Exception occurred while consuming messages: {e}")
        logging.info(e)
    finally:
        consumer.close()
        logging.info("Consumer closed")


def startup():
    logging.info("Starting consumer one...")
    time.sleep(20)
    consume_messages()

if __name__ == "__main__":
    try:
        startup()
    except Exception as e:
        print(f"Exception occurred: {e}")
