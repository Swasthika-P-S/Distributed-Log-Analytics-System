FROM python:3.9-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY . .

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV KAFKA_WAIT_TIME=10

# Run the producer
CMD ["python", "producer.py"]
