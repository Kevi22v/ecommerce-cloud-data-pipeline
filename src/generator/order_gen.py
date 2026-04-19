# src/generator/order_gen.py
import json
import random
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

# --- 1. SETUP ROTATING FILE LOGGING ---
# Max 10 MB per file, keep 4 backups (5 files total = 50 MB max)
log_handler = RotatingFileHandler(
    'order_converter.log', 
    maxBytes=10 * 1024 * 1024,  # 10 MB
    backupCount=4               # Keep 4 older files
)
formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
log_handler.setFormatter(formatter)

logger = logging.getLogger('order_logger')
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

# --- YOUR EXISTING IMPORTS & GLOBALS ---
try:
    from kafka_config import KAFKA_BROKER, TOPIC_CARTS, TOPIC_ORDERS
except ImportError:
    # Fallbacks for testing
    KAFKA_BROKER = "localhost:9092"
    TOPIC_CARTS = "carts"
    TOPIC_ORDERS = "orders"

print(f"Connecting Order Converter to Kafka at {KAFKA_BROKER}...")
print("--> Logs are being written to 'order_converter.log' (Rotates at 10MB, max 50MB)\n")

# 1. SET UP CONSUMER (Read Carts)
consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'order-converter-group',
    'auto.offset.reset': 'latest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC_CARTS])

# 2. SET UP PRODUCER (Send Orders)
producer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'order-producer'
}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        # Catch errors to the log file instead of passing quietly
        logger.error(f"🚨 KAFKA ERROR: {err}")

CONVERSION_RATE = 0.30  

print("Connected! Listening for shopping carts (and passing along the chaos)...")

events_processed = 0

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None: continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF: continue
            else: raise KafkaException(msg.error())
                
        cart_data = json.loads(msg.value().decode('utf-8'))
        events_processed += 1
        
        # Seed random with the cart_id. If a duplicate cart arrives, 
        # it will make the exact same buy/abandon decision!
        random.seed(cart_data["cart_id"]) 
        
        if random.random() <= CONVERSION_RATE:
            
            # Reset the seed so delivery speeds stay random across different orders
            random.seed() 
            delivery_speed = random.choices(["Standard", "Prime", "Urgent"], weights=[0.60, 0.30, 0.10])[0]
            
            # We copy the cart to keep ALL bad data, late timestamps, and new schema fields intact.
            order_data = cart_data.copy()
            
            # We create a deterministic Order ID based on the Cart ID for deduplication tests
            order_data["order_id"] = f"ord_{cart_data['cart_id']}"
            order_data["order_timestamp"] = datetime.utcnow().isoformat()
            order_data["delivery_speed"] = delivery_speed
            order_data["status"] = "Pending"

            order_key = str(order_data.get("order_id", order_data.get("cart_id", "unknown"))).encode('utf-8')
            
            # Send to Kafka
            producer.produce(
                topic=TOPIC_ORDERS,
                key=order_key,
                value=json.dumps(order_data).encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(0)
            
            chaos_flag = order_data.get("chaos_type", "CLEAN")
            logger.info(f"SALE! Order {order_data['order_id'][:12]} generated. [Status: {chaos_flag}]")
        
        else:
            chaos_flag = cart_data.get("chaos_type", "CLEAN")
            logger.info(f"Abandoned: Cart {cart_data['cart_id'][:8]} left behind. [Status: {chaos_flag}]")

        # Terminal update exactly once every 1,000 processed events
        if events_processed % 1000 == 0:
            print(f"⚡ Status: Successfully processed {events_processed} carts from Kafka...")

except KeyboardInterrupt:
    print("\nStopping Order Converter...")
finally:
    consumer.close()
    print("Flushing final messages...")
    producer.flush()