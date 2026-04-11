# src/generator/delivery_gen.py
import json
import random
from datetime import datetime, timedelta
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

from kafka_config import KAFKA_BROKER, TOPIC_ORDERS, TOPIC_DELIVERIES

print(f"Connecting Delivery Fulfiller to Kafka at {KAFKA_BROKER}...")

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
        pass 

print("Connected! Waiting for pending orders to fulfill...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None: continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF: continue
            else: raise KafkaException(msg.error())
                
        order_data = json.loads(msg.value().decode('utf-8'))
        
        # We process instantly, but we calculate a realistic delivery delay
        # to trick the analytics dashboards into seeing real logistics data.
        speed = order_data["delivery_speed"]
        if speed == "Urgent":
            delay_minutes = random.randint(30, 120)       # 30 mins to 2 hours
        elif speed == "Prime":
            delay_minutes = random.randint(720, 2880)     # 12 hours to 2 days
        else: # Standard
            delay_minutes = random.randint(4320, 10080)   # 3 to 7 days
            
        # Parse the original order time and add the math
        order_time = datetime.fromisoformat(order_data["order_timestamp"])
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
        print(f"DELIVERED! Order {order_data['order_id'][:12]} arrived in {delay_minutes//60} hours. [Status: {chaos_flag}]")

except KeyboardInterrupt:
    print("\nStopping Delivery Fulfiller...")
finally:
    consumer.close()
    print("Flushing final messages...")
    producer.flush()