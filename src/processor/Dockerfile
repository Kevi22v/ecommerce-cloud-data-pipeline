# Use a lightweight Python image
FROM python:3.9-slim

# Install Java 11 (IMPORTANT FIX)
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME properly
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY processor.py .

CMD ["python", "processor.py"]