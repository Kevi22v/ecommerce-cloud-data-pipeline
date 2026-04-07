# Use a lightweight Python image
FROM python:3.9-slim

# Install Java (Required for PySpark to run)
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set up our working directory
WORKDIR /app

# Copy our dependencies and code
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY processor.py .

# Run the Spark streaming job
CMD ["python", "processor.py"]