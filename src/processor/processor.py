import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType

# 1. Initialize the Spark Session
spark = SparkSession.builder \
    .appName("EcommerceRealTimeProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.ui.prometheus.enabled", "true") \
    .config("spark.metrics.conf.*.sink.prometheusServlet.class", "org.apache.spark.metrics.sink.PrometheusServlet") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Fetch database credentials securely injected by Kubernetes ConfigMap and Secret
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_URL = f"jdbc:postgresql://{DB_HOST}:5432/ecommerce_db" 
DB_USER = os.environ.get("DB_USER", "dbadmin")
DB_PASS = os.environ.get("DB_PASSWORD", "fallback_pass")

# Pull the dynamic S3 bucket name from the new ConfigMap
S3_BUCKET = os.environ.get("S3_BUCKET", "fallback-bucket-name")

# 2. Define the JSON Schemas
click_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("url", StringType(), True)
])

order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True), # MATCHES GENERATOR
    StructField("timestamp", TimestampType(), True),
    StructField("anomaly_flag", BooleanType(), True) # MATCHES GENERATOR
])

inventory_schema = StructType([
    StructField("update_id", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("warehouse", StringType(), True),
    StructField("quantity_change", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

# 3. Read Real-Time Streams from Kafka
def read_kafka_topic(topic_name, schema):
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka-service:9092") \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "latest") \
        .load()
    
    return df.select(from_json(col("value").cast("string"), schema).alias("data")) \
             .select("data.*") \
             .withColumn("timestamp", expr("to_timestamp(timestamp)"))

# Listen to the exact topics from your ConfigMap
clicks_df = read_kafka_topic("ecommerce_clickstream", click_schema)
orders_df = read_kafka_topic("ecommerce_orders", order_schema)
inventory_df = read_kafka_topic("ecommerce_inventory", inventory_schema)

orders_df = orders_df.withColumn(
    "is_anomaly_injected",
    when(col("amount") > 1000, True).otherwise(False)
)

# 4. Apply Watermarking (Handles late-arriving data)
clicks_watermarked = clicks_df.withWatermark("timestamp", "5 minutes")
orders_watermarked = orders_df.withWatermark("timestamp", "5 minutes")
inventory_watermarked = inventory_df.withWatermark("timestamp", "5 minutes")
# 5. Complex Windowed Join
joined_df = clicks_watermarked.alias("c").join(
    orders_watermarked.alias("o"),
    expr("""
        c.user_id = o.user_id AND
        o.timestamp >= c.timestamp AND
        o.timestamp <= c.timestamp + interval 1 hour
    """)
).select(
    col("c.user_id").alias("user_id"),
    col("c.timestamp").alias("event_time"), 
    col("c.url").alias("page_url"),          
    col("o.order_id"),
    col("o.amount").alias("price"), 
    col("o.is_anomaly_injected"),   
    col("o.timestamp").alias("order_time")
)

# 6. Write to Hot Storage (AWS RDS / PostgreSQL)
def write_to_rds(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", "live_conversions") \
        .option("user", DB_USER) \
        .option("password", DB_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    
def write_orders(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", "orders") \
        .option("user", DB_USER) \
        .option("password", DB_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

def write_clicks(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", "clicks") \
        .option("user", DB_USER) \
        .option("password", DB_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    
def write_inventory(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", "inventory") \
        .option("user", DB_USER) \
        .option("password", DB_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    
rds_query = joined_df.writeStream \
    .foreachBatch(write_to_rds) \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/rds_conversions/") \
    .outputMode("append") \
    .start()

orders_query = orders_df.writeStream \
    .foreachBatch(write_orders) \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/orders/") \
    .start()

clicks_query = clicks_df.writeStream \
    .foreachBatch(write_clicks) \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/clicks/") \
    .start()

inventory_query = inventory_df.writeStream \
    .foreachBatch(write_inventory) \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/inventory/") \
    .start()

# Write to Cold Storage (AWS S3 Data Lake)
s3_query = joined_df.writeStream \
    .format("parquet") \
    .option("path", f"s3a://{S3_BUCKET}/processed-data/") \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/conversions/") \
    .outputMode("append") \
    .start()

# Keep application running
spark.streams.awaitAnyTermination()