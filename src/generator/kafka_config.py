# src/generator/kafka_config.py
import os

KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka-service:9092')

# Topic Names
TOPIC_CARTS = 'carts'
TOPIC_ORDERS = 'orders'
TOPIC_DELIVERIES = 'deliveries'