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

# This tells Python to look for the Kafka door we opened in docker-compose (port 29092)
conf = {'bootstrap.servers': 'localhost:29092'}
producer = Producer(conf)
topic_name = 'ecommerce_orders'

def delivery_report(err, msg):
    """Callback triggered by Kafka to tell us if the message sent successfully."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_order():
    """Generates a single fake e-commerce order."""
    
    # Simulate a 5% chance of an unusually large order (our "Fraud" / "Anomaly" trigger)
    is_anomaly = random.random() < 0.05 
    
    if is_anomaly:
        # Suspiciously high price
        price = round(random.uniform(2500.00, 5000.00), 2)
    else:
        # Normal price range
        price = round(random.uniform(15.00, 300.00), 2)

    order = {
        "order_id": str(uuid.uuid4()),
        "user_id": fake.random_int(min=1000, max=9999),
        "item": random.choice(ITEMS),
        "price": price,
        "timestamp": int(time.time()),
        "is_anomaly_injected": is_anomaly # Flagging it just so we can verify our pipeline works later
    }
    
    return order

if __name__ == "__main__":
    print(f"Starting E-commerce Data Generator. Sending to Kafka topic: {topic_name}...")
    
    try:
        for i in range(100):
            order_data = generate_order()
            
            # We convert the Python dictionary to a JSON string, then encode it to bytes
            producer.produce(
                topic=topic_name, 
                value=json.dumps(order_data).encode('utf-8'), 
                callback=delivery_report
            )
            
            # Ask Kafka to actually send the messages in its queue
            producer.poll(0)
            time.sleep(0.5) 
            
        # Wait for any outstanding messages to be delivered before exiting
        producer.flush()
        print("\nSuccessfully sent 100 records to Kafka. Exiting.")
        
    except KeyboardInterrupt:
        print("\nGenerator stopped manually.")