import os
import json
import psycopg2
from kafka import KafkaConsumer

# 1. Read Cloud Variables
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME", "ecommerce_db")
DB_USER = os.environ.get("DB_USER", "dbadmin")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "password123")
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "ecommerce_orders")

print(f"Connecting to PostgreSQL at {DB_HOST}...")

# 2. Connect to AWS RDS
try:
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    conn.autocommit = True
    cursor = conn.cursor()
    
    # Ensure the table exists in our fresh AWS database
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id VARCHAR(255) PRIMARY KEY,
            user_id INT,
            item VARCHAR(255),
            price DECIMAL(10, 2),
            timestamp BIGINT,
            is_anomaly_injected BOOLEAN
        );
    """)
    print("Database connection and table verified.")
except Exception as e:
    print(f"Fatal Database Error: {e}")
    exit(1)

print(f"Connecting to Kafka at {KAFKA_BROKER}...")

# 3. Connect to Kafka
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest' # Start reading from the oldest unread message
)

print("Successfully connected! Listening for incoming orders...")

# 4. The Infinite Loop (This keeps the container alive forever)
for message in consumer:
    order = message.value
    print(f"Processing Order: {order['order_id']} | Item: {order['item']} | Price: ${order['price']}")
    
    try:
        cursor.execute("""
            INSERT INTO orders (order_id, user_id, item, price, timestamp, is_anomaly_injected)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (order['order_id'], order['user_id'], order['item'], order['price'], order['timestamp'], order['is_anomaly_injected']))
    except psycopg2.Error as e:
        print(f"Failed to insert row: {e}")