FROM python:3.9-slim

RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app and the templates folder
COPY app.py .
COPY templates/ templates/

# Expose the port Flask runs on
EXPOSE 5000

CMD ["python", "app.py"]