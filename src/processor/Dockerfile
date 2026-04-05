FROM python:3.9-slim

# Install system dependencies required for psycopg2 (Postgres driver)
RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dummy_processor.py .

CMD ["python", "dummy_processor.py"]