import os
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

# ==========================================
# 4. INGESTION & DATA QUALITY (The Cleanup)
# ==========================================
def read_and_clean_kafka(topic_name, schema, time_col, unique_id):
    # 1. Read Raw Kafka Stream
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "latest") \
        .load()
    
    # 2. Parse JSON
    parsed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    
    # 3. DATA QUALITY: Drop corrupted negative prices
    validated_df = parsed_df.filter(col("cart_total") >= 0)
    
    # 4. LATE-ARRIVING DATA: Apply Watermark
    watermarked_df = validated_df.withWatermark(time_col, "15 minutes")
    
    # 5. EXACTLY-ONCE SEMANTICS: Drop exact duplicates sent by the Chaos Generator
    deduplicated_df = watermarked_df.dropDuplicates([unique_id, time_col])
    
    return deduplicated_df

clean_carts = read_and_clean_kafka("carts", cart_schema, "timestamp", "cart_id")
clean_orders = read_and_clean_kafka("orders", order_schema, "order_timestamp", "order_id")

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
        
    df.write \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", table_name) \
        .option("user", DB_USER) \
        .option("password", DB_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# Start Postgres Streams
orders_pg_query = clean_orders.writeStream \
    .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "live_orders")) \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/pg_orders/") \
    .start()

revenue_pg_query = revenue_by_location.writeStream \
    .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "revenue_minute_windows")) \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/pg_revenue/") \
    .outputMode("update") \
    .start()

# Start Postgres Stream for Category Revenue
category_pg_query = revenue_by_category.writeStream \
    .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "revenue_category_windows")) \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/pg_category/") \
    .outputMode("update") \
    .start()
# ==========================================
# 7. COLD STORAGE & SCHEMA EVOLUTION (AWS S3)
# ==========================================
# We save raw, unflattened data to S3 using Parquet. 
# mergeSchema=true ensures new columns (like discount_code) are added automatically!
s3_datalake_query = clean_orders.writeStream \
    .format("parquet") \
    .option("path", f"s3a://{S3_BUCKET}/datalake/orders/") \
    .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/s3_orders/") \
    .option("mergeSchema", "true") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()