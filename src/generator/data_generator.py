import os
import json
import time
import random
import uuid
from faker import Faker
from confluent_kafka import Producer

# Initialize Faker to generate random data
fake = Faker()

# A list of typical e-commerce items
ITEMS = ["Laptop", "Wireless Headphones", "Coffee Maker", "Desk Chair", "4K Monitor", "Mechanical Keyboard"]

# ==========================================
# CLOUD NATIVE UPGRADE: Read environment variables
# ==========================================
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "ecommerce_orders")

print(f"Connecting to Kafka Broker at: {KAFKA_BROKER}")

conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(conf)

def delivery_report(err, msg):
    """Callback triggered by Kafka to tell us if the message sent successfully."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_order():
    """Generates a single fake e-commerce order."""
    is_anomaly = random.random() < 0.05 
    
    if is_anomaly:
        price = round(random.uniform(2500.00, 5000.00), 2)
    else:
        price = round(random.uniform(15.00, 300.00), 2)

    order = {
        "order_id": str(uuid.uuid4()),
        "user_id": fake.random_int(min=1000, max=9999),
        "item": random.choice(ITEMS),
        "price": price,
        "timestamp": int(time.time()),
        "is_anomaly_injected": is_anomaly 
    }
    return order

if __name__ == "__main__":
    print(f"Starting E-commerce Data Generator. Sending to Kafka topic: {KAFKA_TOPIC}...")
    
    try:
        # Changed this to a while True loop so it runs forever in Kubernetes
        # instead of just stopping after 100 records!
        while True:
            order_data = generate_order()
            
            producer.produce(
                topic=KAFKA_TOPIC, 
                value=json.dumps(order_data).encode('utf-8'), 
                callback=delivery_report
            )
            
            producer.poll(0)
            time.sleep(0.5) 
            
    except KeyboardInterrupt:
        producer.flush()
        print("\nGenerator stopped manually.")