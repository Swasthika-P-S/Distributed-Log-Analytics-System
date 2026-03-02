FROM python:3.9-slim
LABEL description="Dead-Letter Queue Handler"
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dlq_handler.py .
CMD ["python", "-u", "dlq_handler.py"]
