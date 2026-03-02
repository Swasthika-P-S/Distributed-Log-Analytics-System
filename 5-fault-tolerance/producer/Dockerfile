FROM python:3.9-slim
LABEL description="Fault-Tolerant Kafka Producer"
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ft_producer.py .
CMD ["python", "-u", "ft_producer.py"]
