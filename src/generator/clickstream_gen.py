import os
import json
import time
import random
import uuid
from faker import Faker
from confluent_kafka import Producer
from prometheus_client import start_http_server, Counter

# Initialize Faker
fake = Faker()

# Cloud Native Environment Variables
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_CLICKSTREAM_TOPIC", "ecommerce_clickstream") 

# 👈 NEW: Configurable Speed Control via Kubernetes
SLEEP_MIN = float(os.environ.get("SLEEP_MIN", "0.1"))
SLEEP_MAX = float(os.environ.get("SLEEP_MAX", "0.5"))

CLICKS_GENERATED = Counter('ecommerce_clickstream_total', 'Total clickstream events generated')

print(f"Connecting to Kafka Broker at: {KAFKA_BROKER}")
conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(conf)

# Real-world website routes and devices
PAGES = ["/home", "/category/electronics", "/category/furniture", "/product/laptop", "/cart", "/checkout", "/help"]
OS_LIST = ["iOS", "Android", "Windows", "MacOS", "Linux"]

def delivery_report(err, msg):
    """Callback for message delivery."""
    if err is not None:
        print(f"Delivery failed: {err}")

def generate_click():
    """Generates a single user click event."""
    # CHAOS 1: 20% chance the user is a "Guest" and has no User ID
    is_guest = random.random() < 0.20
    user_id = None if is_guest else fake.random_int(min=1000, max=9999)

    # CHAOS 2: Schema Evolution. 50% chance we capture the browser type.
    include_browser = random.random() < 0.50

    click = {
        "session_id": str(uuid.uuid4()),
        "user_id": user_id,
        "timestamp": int(time.time()),
        "url": random.choice(PAGES),
        "device_os": random.choice(OS_LIST),
        "time_spent_seconds": random.randint(1, 120)
    }

    if include_browser:
        click["browser"] = random.choice(["Chrome", "Safari", "Firefox", "Edge"])

    return click

if __name__ == "__main__":
    print(f"Starting Clickstream Generator. Sending to topic: {KAFKA_TOPIC}...")

    print("Starting Prometheus metrics server on port 5000...")
    start_http_server(5000)

    try:
        while True:
            click_data = generate_click()
            
            # Print a clean, short log to the terminal so we can watch it work
            user_display = click_data['user_id'] if click_data['user_id'] else "GUEST"
            print(f"Click: User [{user_display}] visited {click_data['url']} from {click_data['device_os']}")
            
            producer.produce(
                topic=KAFKA_TOPIC, 
                value=json.dumps(click_data).encode('utf-8'), 
                callback=delivery_report
            )
            
            producer.poll(0)

            CLICKS_GENERATED.inc()
            
            # 👈 NEW: Uses the environment variables to control the speed
            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX)) 
            
    except KeyboardInterrupt:
        producer.flush(timeout=3.0) # 👈 Added the 3-second safety net
        print("\nClickstream Generator stopped manually.")