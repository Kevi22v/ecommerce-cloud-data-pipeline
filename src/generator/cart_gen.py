# src/generator/cart_gen.py
import json, time, random, uuid
from datetime import datetime, timedelta
from confluent_kafka import Producer

from catalog import CATALOG, LOCATIONS, LOCATION_WEIGHTS
from kafka_config import KAFKA_BROKER, TOPIC_CARTS

print(f"Connecting Cart Generator to Kafka at {KAFKA_BROKER}...")

conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'cart-generator-script'
}
producer = Producer(conf)

FLAT_CATALOG = []
for category_name, items in CATALOG.items():
    for item in items:
        FLAT_CATALOG.append({
            "category": category_name,
            "name": item["name"],
            "price": item["price"],
            "weight": item["weight"]
        })

CHAOS_MODE = True

def generate_cart():
    cart_id = str(uuid.uuid4())
    user_id = f"user_{random.randint(1, 5000)}"
    location = random.choices(LOCATIONS, weights=LOCATION_WEIGHTS)[0]
    
    cart_items = []
    cart_total = 0.0
    
    # --- DELETE THE OLD RANDOM.SAMPLE AND PASTE THIS ---
    num_items_to_buy = random.randint(1, 4)
    
    items_list = [item for item in FLAT_CATALOG]
    weights_list = [item["weight"] for item in FLAT_CATALOG]
    
    # Pick items based on their weight
    raw_chosen_items = random.choices(items_list, weights=weights_list, k=num_items_to_buy)
    
    # Quick trick to remove duplicates so we don't get two identical rows in the cart
    chosen_items = list({item['name']: item for item in raw_chosen_items}.values())
    # ---------------------------------------------------
    
    for item in chosen_items:
        qty = random.randint(1, 3)
        subtotal = item["price"] * qty
        cart_total += subtotal
        
        cart_items.append({
            "category": item["category"],
            "item_name": item["name"],
            "unit_price": item["price"],
            "quantity": qty,
            "subtotal": round(subtotal, 2)
        })

    # The Baseline Perfect Cart
    cart = {
        "cart_id": cart_id,
        "user_id": user_id,
        "location": location,
        "timestamp": datetime.utcnow().isoformat(),
        "items": cart_items,
        "cart_total": round(cart_total, 2)
    }

    if CHAOS_MODE:
        chaos_roll = random.random()
        
        # 1. Late-Arriving Data (10% chance)
        if chaos_roll < 0.10:
            late_time = datetime.utcnow() - timedelta(hours=random.randint(1, 5))
            cart["timestamp"] = late_time.isoformat()
            cart["chaos_type"] = "LATE_DATA"
            
        # 2. Schema Evolution (15% chance)
        elif chaos_roll < 0.25:
            cart["discount_code"] = random.choice(["BLACKFRIDAY20", "WELCOME10", "FREESHIP"])
            cart["app_version"] = "v2.1.4"
            cart["chaos_type"] = "NEW_SCHEMA"
            
        # 3. Data Corruption (5% chance)
        elif chaos_roll < 0.30:
            cart["cart_total"] = -50.00 # Impossible negative value!
            cart["chaos_type"] = "CORRUPT_DATA"

    return cart

def delivery_report(err, msg):
    if err is not None:
        print(f"🚨 KAFKA ERROR: {err}")

if __name__ == "__main__":
    print("Started Cart Spammer (🔥 CHAOS MODE ENABLED).")
    print("--> Running in Cloud-Native Mode. To trigger a spike, use:")
    print("--> kubectl scale deployment cart-generator --replicas=15\n")
    
    try:
        while True:
            # Generate exactly 1 cart per loop
            cart_data = generate_cart()
            payload = json.dumps(cart_data).encode('utf-8')
            
            # Send the cart
            producer.produce(topic=TOPIC_CARTS, value=payload, callback=delivery_report)
        
            # 4. Duplicate Chaos (5% chance to send the exact same cart again)
            if CHAOS_MODE and random.random() < 0.05:
                producer.produce(topic=TOPIC_CARTS, value=payload, callback=delivery_report)
                cart_data["chaos_type"] = "DUPLICATE_SENT"
            
            producer.poll(0)
        
            # Terminal Printing Logic
            chaos_flag = cart_data.get("chaos_type", "CLEAN")
            print(f"🛒 Sent Cart {cart_data['cart_id'][:8]}. [Status: {chaos_flag}]")
            
            # Sleep 0.01 seconds (~100 carts per second per pod)
            time.sleep(0.01) 
            
    except KeyboardInterrupt:
        print("\nStopping Cart Generator...")
    finally:
        print("Flushing final messages to Kafka...")
        producer.flush()