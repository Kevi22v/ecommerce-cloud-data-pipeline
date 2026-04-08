# We use the 'bullseye' tag because Debian 11 natively supports Java 11
FROM python:3.9-bullseye

# Because we are on Bullseye, default-jre safely installs Java 11
RUN apt-get update && \
    apt-get install -y default-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY processor.py .

CMD ["python", "processor.py"]