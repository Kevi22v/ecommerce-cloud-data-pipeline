import os
import json
import time
import random
import uuid
from confluent_kafka import Producer
from prometheus_client import start_http_server, Counter

# Cloud Native Environment Variables
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_INVENTORY_TOPIC", "ecommerce_inventory") 

# 👈 NEW: Configurable Speed Control via Kubernetes
SLEEP_MIN = float(os.environ.get("SLEEP_MIN", "0.5"))
SLEEP_MAX = float(os.environ.get("SLEEP_MAX", "1.5"))

INVENTORY_UPDATES = Counter('ecommerce_inventory_total', 'Total inventory updates generated')

print(f"Connecting to Kafka Broker at: {KAFKA_BROKER}")
conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(conf)

WAREHOUSES = ["WH-East-1", "WH-West-2", "WH-South-3"]
SKUS = ["LAPTOP-X1", "PHONE-S22", "DESK-CHAIR", "COFFEE-MUG", "HEADPHONES-V2"]

def delivery_report(err, msg):
    """Callback for message delivery."""
    if err is not None:
        print(f"Delivery failed: {err}")

def generate_inventory_update():
    """Generates an inventory stock update event."""
    current_time = int(time.time())
    
    # CHAOS: 5% chance the scanner was offline and the data is 2 hours old
    is_late = random.random() < 0.05
    event_time = current_time - 7200 if is_late else current_time
    
    update = {
        "update_id": str(uuid.uuid4()),
        "sku": random.choice(SKUS),
        "warehouse": random.choice(WAREHOUSES),
        # Stock mostly drops (-1 to -10), but occasionally they restock (+1 to +5)
        "quantity_change": random.randint(-10, 5), 
        "timestamp": event_time
    }
    
    return update, is_late

if __name__ == "__main__":
    print(f"Starting Inventory Generator. Sending to topic: {KAFKA_TOPIC}...")
    
    print("Starting Prometheus metrics server on port 5000...")
    start_http_server(5000)

    try:
        while True:
            inventory_data, is_late = generate_inventory_update()
            
            # Print a warning in the terminal when we generate late data
            time_status = "⚠️ [DELAYED 2 HOURS]" if is_late else "[REALTIME]"
            
            print(f"Inventory {time_status}: {inventory_data['sku']} at {inventory_data['warehouse']} (Change: {inventory_data['quantity_change']})")
            
            producer.produce(
                topic=KAFKA_TOPIC, 
                value=json.dumps(inventory_data).encode('utf-8'), 
                callback=delivery_report
            )
            
            producer.poll(0)

            INVENTORY_UPDATES.inc()
            
            # 👈 NEW: Uses the environment variables to control the speed
            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX)) 
            
    except KeyboardInterrupt:
        producer.flush(timeout=3.0) 
        print("\nInventory Generator stopped manually.")