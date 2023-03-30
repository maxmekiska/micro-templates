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
    try:
        data = json.dumps(data).encode('utf-8')
        producer.produce('my-topic', value=data)
        producer.flush()
        return {"status": "success", "message": data}
    except Exception as e:
        return {"status": "error", "message": str(e)}
