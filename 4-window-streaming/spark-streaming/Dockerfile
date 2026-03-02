FROM python:3.9-slim

LABEL description="Window-based Spark Structured Streaming"

WORKDIR /app

# Java is required by PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY window_processing.py .

# Run in local[*] mode — no separate Spark cluster needed
CMD ["python", "-u", "window_processing.py"]
