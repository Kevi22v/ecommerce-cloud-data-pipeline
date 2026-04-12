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
    .config("spark.sql.shuffle.partitions", "4") \
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
                discount_code TEXT, app_version TEXT, order_id TEXT PRIMARY KEY,
                order_timestamp TIMESTAMP, delivery_speed TEXT, status TEXT
            );
            
            CREATE TABLE IF NOT EXISTS revenue_minute_windows (
                window_start TIMESTAMP, location TEXT, 
                total_revenue DOUBLE PRECISION, total_orders BIGINT
            );
            
            CREATE TABLE IF NOT EXISTS revenue_category_windows (
                window_start TIMESTAMP, category TEXT, 
                category_revenue DOUBLE PRECISION, items_sold BIGINT
            );
            
            CREATE TABLE IF NOT EXISTS delivery_performance_hourly (
                window_start TIMESTAMP, delivery_speed TEXT, 
                avg_delivery_minutes DOUBLE PRECISION, completed_deliveries BIGINT
            );
            
            CREATE TABLE IF NOT EXISTS abandoned_carts (
                cart_id TEXT, user_id TEXT, 
                cart_time TIMESTAMP, cart_items TEXT
            );
            
            -- Add indexes to make dashboard queries lightning fast
            CREATE INDEX IF NOT EXISTS idx_order_time ON live_orders(order_timestamp);
            CREATE INDEX IF NOT EXISTS idx_rev_time ON revenue_minute_windows(window_start);
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
def read_and_clean_kafka(topic_name, schema, time_col, unique_id, watermark_duration): # <-- Added parameter
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "latest") \
        .load()
    
    parsed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    validated_df = parsed_df.filter(col("cart_total") >= 0)
    
    # Use the dynamic parameter here
    watermarked_df = validated_df.withWatermark(time_col, watermark_duration)
    deduplicated_df = watermarked_df.dropDuplicates([unique_id, time_col])
    
    return deduplicated_df

# Update the calls to pass the specific durations:
clean_carts = read_and_clean_kafka("carts", cart_schema, "timestamp", "cart_id", "1 hour")
clean_orders = read_and_clean_kafka("orders", order_schema, "order_timestamp", "order_id", "7 days")
clean_deliveries = read_and_clean_kafka("deliveries", delivery_schema, "delivery_timestamp", "order_id", "7 days")
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
# 5_NEW. STREAM-TO-STREAM JOIN (Cart Abandonment)
# ==========================================
carts_aliased = clean_carts.selectExpr(
    "cart_id", "user_id", "items as cart_items", "timestamp as cart_time"
)

# We just need the cart_id and order_timestamp from the orders stream to make the match
orders_for_carts = clean_orders.selectExpr(
    "cart_id as matched_cart_id", "order_id", "order_timestamp"
)

# Left Outer Join: Keep the cart, look for an order within 1 hour
abandoned_carts_stream = carts_aliased.join(
    orders_for_carts,
    expr("""
        cart_id = matched_cart_id AND
        order_timestamp >= cart_time AND
        order_timestamp <= cart_time + interval 1 hour
    """),
    "leftOuter"
).filter(col("order_id").isNull()) \
 .select("cart_id", "user_id", "cart_time", "cart_items")

# ==========================================
# 5C. STREAM-TO-STREAM JOIN (Orders + Deliveries)
# ==========================================
# Alias the columns so they don't clash during the join
orders_aliased = clean_orders.selectExpr(
    "order_id", "order_timestamp", "delivery_speed", "location as order_location"
).withWatermark("order_timestamp", "7 days")

deliveries_aliased = clean_deliveries.selectExpr(
    "order_id as del_order_id", "delivery_timestamp", "status as final_status"
).withWatermark("delivery_timestamp", "7 days")

# Join them together!
# We tell Spark: "Match the IDs, and expect the delivery to happen between 0 and 7 days after the order."
lifecycle_stream = orders_aliased.join(
    deliveries_aliased,
    expr("""
        order_id = del_order_id AND
        delivery_timestamp >= order_timestamp AND
        delivery_timestamp <= order_timestamp + interval 7 days
    """)
)

# ==========================================
# 5D. LOGISTICS ANALYTICS (Delivery Performance)
# ==========================================
# Calculate the average delivery time per speed tier over a 1-hour tumbling window
delivery_performance = lifecycle_stream \
    .groupBy(window(col("delivery_timestamp"), "1 hour"), col("delivery_speed")) \
    .agg(
        # Cast timestamps to seconds (double), subtract them, divide by 60 for minutes
        expr("avg(cast(delivery_timestamp as double) - cast(order_timestamp as double)) / 60").alias("avg_delivery_minutes"),
        count("order_id").alias("completed_deliveries")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("delivery_speed"),
        col("avg_delivery_minutes"),
        col("completed_deliveries")
    )

# ==========================================
# 6. HOT STORAGE (PostgreSQL)
# ==========================================
def write_to_postgres(df, epoch_id, table_name):
    # Flatten the complex array into a JSON string so PostgreSQL can store it easily
    if "items" in df.columns:
        df = df.withColumn("items", to_json(col("items")))
        
    df.write \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", table_name) \
        .option("user", DB_USER) \
        .option("password", DB_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# Start Postgres Streams (Now with 10-second micro-batch triggers!)
orders_pg_query = clean_orders.writeStream \
    .trigger(processingTime="10 seconds") \
    .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "live_orders")) \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/pg_orders/") \
    .start()

revenue_pg_query = revenue_by_location.writeStream \
    .trigger(processingTime="10 seconds") \
    .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "revenue_minute_windows")) \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/pg_revenue/") \
    .outputMode("update") \
    .start()

category_pg_query = revenue_by_category.writeStream \
    .trigger(processingTime="10 seconds") \
    .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "revenue_category_windows")) \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/pg_category/") \
    .outputMode("update") \
    .start()

delivery_pg_query = delivery_performance.writeStream \
    .trigger(processingTime="10 seconds") \
    .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "delivery_performance_hourly")) \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/pg_delivery_perf/") \
    .outputMode("append") \
    .start()

abandoned_pg_query = abandoned_carts_stream.writeStream \
    .trigger(processingTime="10 seconds") \
    .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "abandoned_carts")) \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/pg_abandoned_carts/") \
    .outputMode("append") \
    .start()

# ==========================================
# 7. COLD STORAGE & SCHEMA EVOLUTION (AWS S3)
# ==========================================
# We save raw, unflattened data to S3 using Parquet. 
# mergeSchema=true ensures new columns (like discount_code) are added automatically!
s3_datalake_query = clean_orders.writeStream \
    .format("parquet") \
    .trigger(processingTime="2 minutes") \
    .option("path", f"s3a://{S3_BUCKET}/datalake/orders/") \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/s3_orders/") \
    .option("mergeSchema", "true") \
    .outputMode("append") \
    .start()

# Save the joined, end-to-end transaction lifecycle to S3 Datalake
s3_lifecycle_query = lifecycle_stream.writeStream \
    .format("parquet") \
    .trigger(processingTime="2 minutes") \
    .option("mergeSchema", "true") \
    .option("path", f"s3a://{S3_BUCKET}/datalake/lifecycle/") \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/s3_lifecycle/") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()