from kafka import KafkaProducer
from faker import Faker
import json
import time
import random

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode()
)

PRODUCTS = ['Laptop', 'Phone', 'Tablet', 'Monitor', 'Keyboard']

def generate_event():
    return {
        "event_id": fake.uuid4(),
        "product": random.choice(PRODUCTS),
        "quantity": random.randint(1, 10),
        "price": round(random.uniform(10, 2000), 2),
        "timestamp": fake.iso8601(),
        "region": fake.country_code()
    }

if __name__ == "__main__":
    print("Producing sales events...")
    while True:
        event = generate_event()
        producer.send('sales_events', value=event)
        print(f"Sent: {event['product']} x{event['quantity']} @ ${event['price']}")
        time.sleep(0.5)