# We use the 'bullseye' tag because Debian 11 natively supports Java 11
FROM python:3.9-bullseye

# 👇 FIX 1: Force Python logs to stream to the terminal instantly
ENV PYTHONUNBUFFERED=1

# Install Java 11
RUN apt-get update && \
    apt-get install -y default-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app

# Install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# We run a dummy PySpark command during the build phase. 
# This forces Spark to download the Kafka, AWS, and Postgres JARs from the internet
# and save them inside the Docker image (/root/.ivy2/jars) so pods boot instantly.
RUN python -c "from pyspark.sql import SparkSession; \
    SparkSession.builder \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0') \
    .getOrCreate()"

# Copy the actual processing script
COPY processor.py .

CMD ["python", "processor.py"]