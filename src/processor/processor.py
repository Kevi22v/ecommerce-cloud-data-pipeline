import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1. Initialize the Spark Session
# 1. Initialize the Spark Session
# 1. Initialize the Spark Session
spark = SparkSession.builder \
    .appName("EcommerceRealTimeProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Fetch database credentials securely injected by Kubernetes ConfigMap and Secret
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_URL = f"jdbc:postgresql://{DB_HOST}:5432/ecommerce_db" 
DB_USER = os.environ.get("DB_USER", "dbadmin")
DB_PASS = os.environ.get("DB_PASSWORD", "fallback_pass")

# 2. Define the JSON Schemas
click_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("page_url", StringType(), True)
])

order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("item", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", TimestampType(), True), # Using timestamp as time
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
             .select("data.*")

# Listen to the exact topics from your ConfigMap
clicks_df = read_kafka_topic("ecommerce_clickstream", click_schema)
orders_df = read_kafka_topic("ecommerce_orders", order_schema)

# 4. Apply Watermarking (Handles late-arriving data)
clicks_watermarked = clicks_df.withWatermark("event_time", "5 minutes")
orders_watermarked = orders_df.withWatermark("timestamp", "5 minutes")

# 5. Complex Windowed Join
joined_df = clicks_watermarked.alias("c").join(
    orders_watermarked.alias("o"),
    expr("""
        c.user_id = o.user_id AND
        o.timestamp >= c.event_time AND
        o.timestamp <= c.event_time + interval 1 hour
    """)
)

# 6. Write to Hot Storage (AWS RDS / PostgreSQL)
def write_to_rds(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", "live_conversions") \
        .option("user", DB_USER) \
        .option("password", DB_PASS) \
        .mode("append") \
        .save()

rds_query = joined_df.writeStream \
    .foreachBatch(write_to_rds) \
    .outputMode("append") \
    .start()

# Pull the dynamic S3 bucket name from the new ConfigMap
S3_BUCKET = os.environ.get("S3_BUCKET", "fallback-bucket-name")

# Write to Cold Storage (AWS S3 Data Lake)
s3_query = joined_df.writeStream \
    .format("parquet") \
    .option("path", f"s3a://{S3_BUCKET}/processed-data/") \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/") \
    .outputMode("append") \
    .start()

# Keep application running
spark.streams.awaitAnyTermination()