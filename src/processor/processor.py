import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, window, sum, count, to_json, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, ArrayType


# ==========================================
# 1. INITIALIZE SPARK
# ==========================================
spark = SparkSession.builder \
    .appName("EcommerceChaosProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.ui.prometheus.enabled", "true") \
    .config("spark.metrics.conf.*.sink.prometheusServlet.class", "org.apache.spark.metrics.sink.PrometheusServlet") \
    .config("spark.sql.shuffle.partitions", "30") \
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
    .getOrCreate()

# Change this from "WARN" to "INFO"
spark.sparkContext.setLogLevel("INFO")

# ==========================================
# 2. CREDENTIALS & CONFIG
# ==========================================
DB_HOST = os.environ.get("DB_HOST", "postgres-service") # Matches Kubernetes DNS
DB_URL = f"jdbc:postgresql://{DB_HOST}:5432/ecommerce_db" 
DB_USER = os.environ.get("DB_USER", "dbadmin")
DB_PASS = os.environ.get("DB_PASSWORD", "fallback_pass")
S3_BUCKET = os.environ.get("S3_BUCKET", "fallback-bucket-name")
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka-service:9092")


# ==========================================
# 2B. DATABASE INITIALIZATION (Self-Healing)
# ==========================================
def initialize_database():
    print("⏳ Checking/Creating PostgreSQL Tables...")
    try:
        # Connect to the database
        conn = psycopg2.connect(
            host=DB_HOST, 
            database="ecommerce_db", 
            user=DB_USER, 
            password=DB_PASS
        )
        cur = conn.cursor()
        
        # Create tables if they don't exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS live_orders (
                cart_id TEXT, user_id TEXT, location TEXT, timestamp TIMESTAMP,
                items TEXT, cart_total DOUBLE PRECISION, chaos_type TEXT,
                discount_code TEXT, app_version TEXT, order_id TEXT,
                order_timestamp TIMESTAMP, delivery_speed TEXT, status TEXT
            );
            
            CREATE TABLE IF NOT EXISTS raw_deliveries (
                order_id TEXT,
                delivery_timestamp TIMESTAMP,
                delivery_speed TEXT,
                status TEXT
            );
            
            CREATE TABLE IF NOT EXISTS raw_carts (
                cart_id TEXT,
                user_id TEXT,
                timestamp TIMESTAMP,
                items TEXT
            );
            
            CREATE TABLE IF NOT EXISTS revenue_minute_windows (
                window_start TIMESTAMP, location TEXT, 
                total_revenue DOUBLE PRECISION, total_orders BIGINT
            );
            
            CREATE TABLE IF NOT EXISTS revenue_category_windows (
                window_start TIMESTAMP, category TEXT, 
                category_revenue DOUBLE PRECISION, items_sold BIGINT
            );
            
            -- ELT View 1: Abandoned Carts
            DROP TABLE IF EXISTS abandoned_carts CASCADE;
            CREATE OR REPLACE VIEW abandoned_carts AS
            SELECT 
                c.cart_id, 
                c.user_id, 
                c.timestamp AS cart_time, 
                c.items AS cart_items
            FROM raw_carts c
            LEFT JOIN live_orders o ON c.cart_id = o.cart_id
            WHERE o.order_id IS NULL;
            
            -- ELT View 2: Delivery Performance
            CREATE OR REPLACE VIEW live_delivery_performance AS
            SELECT 
                date_trunc('hour', d.delivery_timestamp) AS window_start,
                o.delivery_speed,
                AVG(EXTRACT(EPOCH FROM (d.delivery_timestamp - o.order_timestamp)) / 60) AS avg_delivery_minutes,
                COUNT(o.order_id) AS completed_deliveries
            FROM live_orders o
            JOIN raw_deliveries d ON o.order_id = d.order_id
            GROUP BY date_trunc('hour', d.delivery_timestamp), o.delivery_speed;
            
            -- Indexes for speed (Replacing the Primary Keys!)
            CREATE INDEX IF NOT EXISTS idx_order_id ON live_orders(order_id);
            CREATE INDEX IF NOT EXISTS idx_order_time ON live_orders(order_timestamp);
            CREATE INDEX IF NOT EXISTS idx_rev_time ON revenue_minute_windows(window_start);
            CREATE INDEX IF NOT EXISTS idx_del_order ON raw_deliveries(order_id);
            CREATE INDEX IF NOT EXISTS idx_cart_match ON raw_carts(cart_id);
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Database tables ready!")
        
    except Exception as e:
        print(f"⚠️ Database initialization failed: {e}")

# Run it before Spark does anything!
initialize_database()

# ==========================================
# 3. SCHEMA DEFINITIONS (With Schema Evolution buffers)
# ==========================================
item_schema = ArrayType(StructType([
    StructField("category", StringType(), True),
    StructField("item_name", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("subtotal", DoubleType(), True)
]))

cart_schema = StructType([
    StructField("cart_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("items", item_schema, True),
    StructField("cart_total", DoubleType(), True),
    StructField("chaos_type", StringType(), True),
    StructField("discount_code", StringType(), True), # Chaos: Schema Evolution
    StructField("app_version", StringType(), True)    # Chaos: Schema Evolution
])

order_schema = cart_schema.add("order_id", StringType(), True) \
                          .add("order_timestamp", TimestampType(), True) \
                          .add("delivery_speed", StringType(), True) \
                          .add("status", StringType(), True)

delivery_schema = order_schema.add("delivery_timestamp", TimestampType(), True)
# ==========================================
# 4. INGESTION & DATA QUALITY (The Cleanup)
# ==========================================
def read_and_clean_kafka(topic_name, schema, time_col, unique_id, watermark_duration, filter_cart=True): 
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", 15000) \
        .load()
    
    parsed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    validated_df = parsed_df.filter(col("cart_total") >= 0) if filter_cart else parsed_df
    
    watermarked_df = validated_df.withWatermark(time_col, watermark_duration)
    deduplicated_df = watermarked_df.dropDuplicates([unique_id, time_col])
    
    return deduplicated_df

# Update the calls to pass the specific durations:
clean_carts = read_and_clean_kafka("carts", cart_schema, "timestamp", "cart_id", "5 minutes")
clean_orders = read_and_clean_kafka("orders", order_schema, "order_timestamp", "order_id", "5 minutes")
clean_deliveries = read_and_clean_kafka("deliveries", delivery_schema, "delivery_timestamp", "order_id", "5 minutes", filter_cart=False)
# ==========================================
# 5. STREAMING AGGREGATIONS (Windowing)
# ==========================================
# Calculate Total Revenue per minute, per physical location
revenue_by_location = clean_orders \
    .groupBy(window(col("order_timestamp"), "1 minute"), col("location")) \
    .agg(
        sum("cart_total").alias("total_revenue"),
        count("order_id").alias("total_orders")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("location"),
        col("total_revenue"),
        col("total_orders")
    )

# ==========================================
# 5B. STREAMING AGGREGATIONS (Category Revenue)
# ==========================================
# First, we 'explode' the items array. If a cart has 3 items, this turns it into 3 separate rows!
exploded_orders = clean_orders.withColumn("item", explode(col("items")))

# Now we calculate Total Revenue per minute, per Category
revenue_by_category = exploded_orders \
    .groupBy(window(col("order_timestamp"), "1 minute"), col("item.category").alias("category")) \
    .agg(
        sum("item.subtotal").alias("category_revenue"),
        sum("item.quantity").alias("items_sold")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("category"),
        col("category_revenue"),
        col("items_sold")
    )
# ==========================================
# 6. HOT STORAGE (PostgreSQL)
# ==========================================
def write_to_postgres(df, epoch_id, table_name):
    # Flatten the complex array into a JSON string so PostgreSQL can store it easily
    if "items" in df.columns:
        df = df.withColumn("items", to_json(col("items")))
        
    # FORCE the dataframe to match the PostgreSQL table EXACTLY before writing
    if table_name == "live_orders":
        df = df.select("cart_id", "user_id", "location", "timestamp", 
                       "items", "cart_total", "chaos_type", "discount_code", 
                       "app_version", "order_id", "order_timestamp", 
                       "delivery_speed", "status")
    elif table_name == "raw_deliveries":
        df = df.select("order_id", "delivery_timestamp", "delivery_speed", "status")
    elif table_name == "raw_carts":
        df = df.select("cart_id", "user_id", "timestamp", "items")
    df.write \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", table_name) \
        .option("user", DB_USER) \
        .option("password", DB_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .option("batchsize", "1000") \
        .option("numPartitions", "4") \
        .mode("append") \
        .save()
    
# Start Postgres streams with staggered trigger intervals to reduce contention.
orders_pg_query = clean_orders.writeStream \
    .trigger(processingTime="5 seconds") \
    .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "live_orders")) \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/pg_orders/") \
    .start()

revenue_pg_query = revenue_by_location.writeStream \
    .trigger(processingTime="30 seconds") \
    .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "revenue_minute_windows")) \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/pg_revenue/") \
    .outputMode("update") \
    .start()

category_pg_query = revenue_by_category.writeStream \
    .trigger(processingTime="30 seconds") \
    .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "revenue_category_windows")) \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/pg_category/") \
    .outputMode("update") \
    .start()

delivery_pg_query = clean_deliveries.writeStream \
    .trigger(processingTime="2 minutes") \
    .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "raw_deliveries")) \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/pg_raw_deliveries/") \
    .outputMode("append") \
    .start()

cart_pg_query = clean_carts.writeStream \
    .trigger(processingTime="5 minutes") \
    .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "raw_carts")) \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/pg_raw_carts/") \
    .outputMode("append") \
    .start()

# ==========================================
# 7. COLD STORAGE & SCHEMA EVOLUTION (AWS S3)
# ==========================================
# We save raw, unflattened data to S3 using Parquet. 
# mergeSchema=true ensures new columns (like discount_code) are added automatically!
s3_datalake_query = clean_orders.writeStream \
    .format("parquet") \
    .trigger(processingTime="5 minutes") \
    .option("path", f"s3a://{S3_BUCKET}/datalake/orders/") \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/s3_orders/") \
    .option("mergeSchema", "true") \
    .outputMode("append") \
    .start()

# Save the joined, end-to-end transaction lifecycle to S3 Datalake
s3_deliveries_query = clean_deliveries.writeStream \
    .format("parquet") \
    .trigger(processingTime="5 minutes") \
    .option("path", f"s3a://{S3_BUCKET}/datalake/deliveries/") \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/s3_deliveries/") \
    .option("mergeSchema", "true") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()