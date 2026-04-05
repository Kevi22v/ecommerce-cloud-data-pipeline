import json
import psycopg2
from confluent_kafka import Consumer

# --- 1. Connect to PostgreSQL ---
print("Connecting to PostgreSQL...")
conn = psycopg2.connect(
    host="localhost",
    database="ecommerce_db",
    user="admin",
    password="password123",
    port="5432"
)
cursor = conn.cursor()

# Create the storage table if it doesn't exist yet
cursor.execute("""
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(255) PRIMARY KEY,
    user_id INT,
    item VARCHAR(255),
    price FLOAT,
    timestamp BIGINT,
    is_anomaly_injected BOOLEAN
);
""")
conn.commit()
print("Postgres table 'orders' is ready.")

# --- 2. Configure the Kafka Consumer ---
print("Connecting to Kafka...")
consumer = Consumer({
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'python-processor-group',
    # 'earliest' tells Kafka to read from the beginning, so we catch the 100 messages you already sent!
    'auto.offset.reset': 'earliest' 
})

consumer.subscribe(['ecommerce_orders'])
print("Listening for messages on topic 'ecommerce_orders'...")

# --- 3. The Processing Loop ---
try:
    while True:
        # Wait up to 1 second for a new message
        msg = consumer.poll(1.0) 
        
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Decode the JSON message from Kafka
        data = json.loads(msg.value().decode('utf-8'))
        
        # Insert the data into Postgres
        cursor.execute("""
            INSERT INTO orders (order_id, user_id, item, price, timestamp, is_anomaly_injected)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING;
        """, (data['order_id'], data['user_id'], data['item'], data['price'], data['timestamp'], data['is_anomaly_injected']))
        
        conn.commit()
        print(f"Saved order {data['order_id']} to Postgres")

except KeyboardInterrupt:
    print("\nProcessor stopped manually.")
finally:
    # Clean up connections when we stop the script
    consumer.close()
    cursor.close()
    conn.close()