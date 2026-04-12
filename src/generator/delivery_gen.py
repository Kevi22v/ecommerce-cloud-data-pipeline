# src/generator/delivery_gen.py
import json
import random
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

# --- 1. SETUP ROTATING FILE LOGGING ---
# Max 10 MB per file, keep 4 backups (5 files total = 50 MB max)
log_handler = RotatingFileHandler(
    'delivery_fulfiller.log', 
    maxBytes=10 * 1024 * 1024,  # 10 MB
    backupCount=4               # Keep 4 older files
)
formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
log_handler.setFormatter(formatter)

logger = logging.getLogger('delivery_logger')
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

# --- YOUR EXISTING IMPORTS & GLOBALS ---
try:
    from kafka_config import KAFKA_BROKER, TOPIC_ORDERS, TOPIC_DELIVERIES
except ImportError:
    # Fallbacks for testing
    KAFKA_BROKER = "localhost:9092"
    TOPIC_ORDERS = "orders"
    TOPIC_DELIVERIES = "deliveries"

print(f"Connecting Delivery Fulfiller to Kafka at {KAFKA_BROKER}...")
print("--> Logs are being written to 'delivery_fulfiller.log' (Rotates at 10MB, max 50MB)\n")

# 1. SET UP CONSUMER (Read Orders)
consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'delivery-fulfillment-group', # <--- Auto-scales just like orders!
    'auto.offset.reset': 'latest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC_ORDERS])

# 2. SET UP PRODUCER (Send Deliveries)
producer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'delivery-producer'
}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        # Catch errors to the log file instead of passing quietly
        logger.error(f"🚨 KAFKA ERROR: {err}")

print("Connected! Waiting for pending orders to fulfill...")

events_processed = 0

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None: continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF: continue
            else: raise KafkaException(msg.error())
                
        order_data = json.loads(msg.value().decode('utf-8'))
        events_processed += 1
        
        # We process instantly, but we calculate a realistic delivery delay
        # to trick the analytics dashboards into seeing real logistics data.
        speed = order_data.get("delivery_speed", "Standard")
        if speed == "Urgent":
            delay_minutes = random.randint(30, 120)       # 30 mins to 2 hours
        elif speed == "Prime":
            delay_minutes = random.randint(720, 2880)     # 12 hours to 2 days
        else: # Standard
            delay_minutes = random.randint(4320, 10080)   # 3 to 7 days
            
        # Parse the original order time and add the math
        # Fallback to current time if order_timestamp is missing due to chaos
        raw_timestamp = order_data.get("order_timestamp", datetime.utcnow().isoformat())
        order_time = datetime.fromisoformat(raw_timestamp)
        delivery_time = order_time + timedelta(minutes=delay_minutes)
        
        # Copy the order to keep the Chaos Data intact!
        delivery_data = order_data.copy()
        delivery_data["delivery_timestamp"] = delivery_time.isoformat()
        delivery_data["status"] = "Delivered"
        
        # Send to Kafka instantly
        producer.produce(
            topic=TOPIC_DELIVERIES,
            value=json.dumps(delivery_data).encode('utf-8'),
            callback=delivery_report
        )
        producer.poll(0)
        
        chaos_flag = delivery_data.get("chaos_type", "CLEAN")
        logger.info(f"DELIVERED! Order {order_data.get('order_id', 'UNKNOWN')[:12]} arrived in {delay_minutes//60} hours. [Status: {chaos_flag}]")

        # Terminal update exactly once every 1,000 processed events
        if events_processed % 1000 == 0:
            print(f"⚡ Status: Successfully processed {events_processed} deliveries from Kafka...")

except KeyboardInterrupt:
    print("\nStopping Delivery Fulfiller...")
finally:
    consumer.close()
    print("Flushing final messages...")
    producer.flush()