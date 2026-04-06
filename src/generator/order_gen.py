import os
import json
import time
import random
import uuid
from confluent_kafka import Producer

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_ORDER_TOPIC", "ecommerce_orders")

print(f"Connecting to Kafka Broker at: {KAFKA_BROKER}")
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")

def generate_order():
    """Generates an order with occasional Fraud Chaos."""
    # CHAOS 1: 1% chance of a massive fraudulent transaction
    is_fraud = random.random() < 0.01
    
    # Normal orders are $10 - $500. Fraud orders are $5,000 - $20,000
    amount = round(random.uniform(5000.0, 20000.0), 2) if is_fraud else round(random.uniform(10.0, 500.0), 2)

    return {
        "order_id": str(uuid.uuid4()),
        "user_id": random.randint(1000, 9999),
        "amount": amount,
        "timestamp": int(time.time()),
        # We leave a hidden flag here, but Spark will have to catch the math anomaly!
        "anomaly_flag": is_fraud 
    }

if __name__ == "__main__":
    print(f"Starting Order Generator on topic: {KAFKA_TOPIC}...")
    
    # CHAOS 2: The Flash Sale Simulator
    # Every 60 seconds, the system will go absolutely crazy for 5 seconds
    flash_sale_active = False
    flash_sale_end_time = 0
    
    try:
        while True:
            current_time = time.time()
            
            # Check if we should trigger a Flash Sale (10% chance every loop if not active)
            if not flash_sale_active and random.random() < 0.05:
                print("\n🚨🚨 FLASH SALE TRIGGERED! MASSIVE TRAFFIC SPIKE! 🚨🚨\n")
                flash_sale_active = True
                flash_sale_end_time = current_time + 5 # Lasts for 5 seconds

            # Check if Flash Sale is over
            if flash_sale_active and current_time > flash_sale_end_time:
                print("\n📉 Flash sale ended. Returning to normal traffic.\n")
                flash_sale_active = False

            order_data = generate_order()
            
            # Print warnings for Fraud
            if order_data["anomaly_flag"]:
                print(f"⚠️ FRAUD ALERT: Massive order of ${order_data['amount']} from User [{order_data['user_id']}]")
            elif not flash_sale_active:
                # Only print normal orders if we aren't in a flash sale (too much text otherwise!)
                print(f"Order: User [{order_data['user_id']}] spent ${order_data['amount']}")
            
            producer.produce(
                topic=KAFKA_TOPIC, 
                value=json.dumps(order_data).encode('utf-8'), 
                callback=delivery_report
            )
            producer.poll(0)
            
            # SPEED CHAOS: 
            # Normal traffic: 1 order every 0.5 to 2 seconds
            # Flash Sale: 1 order every 0.001 seconds (Maximum speed!)
            sleep_time = 0.001 if flash_sale_active else random.uniform(0.5, 2.0)
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        producer.flush(timeout=3.0)
        print("\nOrder Generator stopped manually.")