FROM python:3.9-slim
LABEL description="Fault-Tolerant Kafka Consumer"
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ft_consumer.py .
CMD ["python", "-u", "ft_consumer.py"]
