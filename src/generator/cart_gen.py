# src/generator/cart_gen.py
import json, time, random, uuid, logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from confluent_kafka import Producer

# --- 1. SETUP ROTATING FILE LOGGING ---
# Max 10 MB per file, keep 4 backups (5 files total = 50 MB max)
log_handler = RotatingFileHandler(
    'cart_generator.log', 
    maxBytes=10 * 1024 * 1024,  # 10 MB
    backupCount=4               # Keep 4 older files
)
formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
log_handler.setFormatter(formatter)

logger = logging.getLogger('cart_logger')
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

# --- YOUR EXISTING IMPORTS & GLOBALS ---
try:
    from catalog import CATALOG, LOCATIONS, LOCATION_WEIGHTS
    from kafka_config import KAFKA_BROKER, TOPIC_CARTS
except ImportError:
    # Fallbacks for testing
    KAFKA_BROKER = "localhost:9092"
    TOPIC_CARTS = "carts"
    CATALOG = {"electronics": [{"name": "Laptop", "price": 1000.0, "weight": 5}]}
    LOCATIONS = ["NY", "SF"]
    LOCATION_WEIGHTS = [0.6, 0.4]

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
    
    num_items_to_buy = random.randint(1, 4)
    items_list = [item for item in FLAT_CATALOG]
    weights_list = [item["weight"] for item in FLAT_CATALOG]
    
    raw_chosen_items = random.choices(items_list, weights=weights_list, k=num_items_to_buy)
    chosen_items = list({item['name']: item for item in raw_chosen_items}.values())
    
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
        if chaos_roll < 0.10:
            late_time = datetime.utcnow() - timedelta(hours=random.randint(1, 5))
            cart["timestamp"] = late_time.isoformat()
            cart["chaos_type"] = "LATE_DATA"
        elif chaos_roll < 0.25:
            cart["discount_code"] = random.choice(["BLACKFRIDAY20", "WELCOME10", "FREESHIP"])
            cart["app_version"] = "v2.1.4"
            cart["chaos_type"] = "NEW_SCHEMA"
        elif chaos_roll < 0.30:
            cart["cart_total"] = -50.00 
            cart["chaos_type"] = "CORRUPT_DATA"

    return cart

def delivery_report(err, msg):
    if err is not None:
        # Replaced print with logger.error
        logger.error(f"🚨 KAFKA ERROR: {err}")

if __name__ == "__main__":
    print("Started Cart Spammer (🔥 CHAOS MODE ENABLED).")
    print("--> Logs are being written to 'cart_generator.log' (Rotates at 10MB, max 50MB)")
    print("--> Target: 1000 events/sec\n")
    
    # --- 2. BATCH RATE LIMITING CONFIG ---
    TARGET_RATE = 2000
    BATCH_SIZE = 100
    IDEAL_BATCH_TIME = BATCH_SIZE / TARGET_RATE  # 0.1 seconds per 100 items
    
    events_sent_this_batch = 0
    total_events_sent = 0
    start_time = time.time()
    
    try:
        while True:
            cart_data = generate_cart()
            payload = json.dumps(cart_data).encode('utf-8')
            
            # Send primary cart
            producer.produce(topic=TOPIC_CARTS, value=payload, callback=delivery_report)
            events_sent_this_batch += 1
            
            # Log primary cart to background file
            chaos_flag = cart_data.get("chaos_type", "CLEAN")
            logger.info(f"🛒 Sent Cart {cart_data['cart_id'][:8]}. [Status: {chaos_flag}]")
        
            # Send and log chaos duplicate
            if CHAOS_MODE and random.random() < 0.05:
                producer.produce(topic=TOPIC_CARTS, value=payload, callback=delivery_report)
                events_sent_this_batch += 1
                logger.info(f"🛒 Sent Cart {cart_data['cart_id'][:8]}. [Status: DUPLICATE_SENT]")
            
            producer.poll(0)
            
            # --- 3. BATCH TIMING EXECUTION ---
            if events_sent_this_batch >= BATCH_SIZE:
                elapsed = time.time() - start_time
                time_to_sleep = IDEAL_BATCH_TIME - elapsed
                
                # Sleep exact remaining milliseconds to hit 1000/sec target
                if time_to_sleep > 0:
                    time.sleep(time_to_sleep)
                
                total_events_sent += events_sent_this_batch
                
                # Print to terminal exactly once per second so you know it's working
                if total_events_sent % 1000 == 0:
                    print(f"⚡ Status: Successfully sent {total_events_sent} total events to Kafka...")
                
                # Reset counters for the next batch
                events_sent_this_batch = 0
                start_time = time.time()
            
    except KeyboardInterrupt:
        print("\nStopping Cart Generator...")
    finally:
        print("Flushing final messages to Kafka...")
        producer.flush()