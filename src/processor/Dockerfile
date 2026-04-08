FROM python:3.9-slim

# Install the default headless Java (bulletproof against OS updates)
RUN apt-get update && \
    apt-get install -y default-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Point to the default Java path
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY processor.py .

CMD ["python", "processor.py"]