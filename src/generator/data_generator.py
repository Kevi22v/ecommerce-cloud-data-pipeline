import json
import time
import random
from faker import Faker
import uuid

# Initialize Faker to generate random data
fake = Faker()

# A list of typical e-commerce items
ITEMS = ["Laptop", "Wireless Headphones", "Coffee Maker", "Desk Chair", "4K Monitor", "Mechanical Keyboard"]

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
    print("Starting E-commerce Data Generator...")
    print("Press Ctrl+C to stop.")
    
    try:
        # Generate exactly 100 records for our Phase 4 "Safety Net" test
        for i in range(100):
            order_data = generate_order()
            # Print the JSON to the terminal so we can see it working
            print(json.dumps(order_data))
            
            # Pause for half a second to simulate steady website traffic
            time.sleep(0.5) 
            
        print("\nSuccessfully generated 100 records. Exiting.")
        
    except KeyboardInterrupt:
        print("\nGenerator stopped manually.")