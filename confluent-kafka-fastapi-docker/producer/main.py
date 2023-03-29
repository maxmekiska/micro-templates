import json
from confluent_kafka import Producer
from fastapi import FastAPI


app = FastAPI()

producer_conf = {
    'bootstrap.servers': 'kafka:29092',
    'client.id': 'my-app'
}

producer = Producer(producer_conf)

@app.post("/produce")
def produce(data: dict):
    producer.produce('my-topic', value=json.dumps(data).encode('utf-8'))
    producer.flush()
    return {"status": "success"}
