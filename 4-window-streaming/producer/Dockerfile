FROM python:3.9-slim

LABEL description="Window-Streaming Log Producer"

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

CMD ["python", "-u", "producer.py"]
