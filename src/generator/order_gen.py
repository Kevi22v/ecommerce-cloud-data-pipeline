import os
import json
import time
import random
import uuid
from confluent_kafka import Producer
from prometheus_client import start_http_server, Counter

# Cloud Native Environment Variables
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_ORDER_TOPIC", "ecommerce_orders")

# 👈 NEW: Configurable Speed Control via Kubernetes
SLEEP_MIN = float(os.environ.get("SLEEP_MIN", "0.5"))
SLEEP_MAX = float(os.environ.get("SLEEP_MAX", "2.0"))

ORDERS_GENERATED = Counter('ecommerce_orders_total', 'Total orders generated')

print(f"Connecting to Kafka Broker at: {KAFKA_BROKER}")
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")

def generate_order():
    """Generates an order with occasional Fraud Chaos."""
    # We keep the fraud logic because it tests Spark's anomaly detection!
    is_fraud = random.random() < 0.01 
    amount = round(random.uniform(5000.0, 20000.0), 2) if is_fraud else round(random.uniform(10.0, 500.0), 2)

    return {
        "order_id": str(uuid.uuid4()),
        "user_id": random.randint(1000, 9999),
        "amount": amount,
        "timestamp": int(time.time()),
        "anomaly_flag": is_fraud 
    }

if __name__ == "__main__":
    print(f"Starting Order Generator on topic: {KAFKA_TOPIC}...")
    print("Starting Prometheus metrics server on port 5000...")
    start_http_server(5000)

    try:
        while True:
            order_data = generate_order()
            
            # Print warnings for Fraud
            if order_data["anomaly_flag"]:
                print(f"⚠️ FRAUD ALERT: Massive order of ${order_data['amount']} from User [{order_data['user_id']}]")
            else:
                print(f"Order: User [{order_data['user_id']}] spent ${order_data['amount']}")
            
            producer.produce(
                topic=KAFKA_TOPIC, 
                value=json.dumps(order_data).encode('utf-8'), 
                callback=delivery_report
            )
            producer.poll(0)
            
            ORDERS_GENERATED.inc()

            # 👈 NEW: Uses the environment variables to control the speed
            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
            
    except KeyboardInterrupt:
        producer.flush(timeout=3.0)
        print("\nOrder Generator stopped manually.")