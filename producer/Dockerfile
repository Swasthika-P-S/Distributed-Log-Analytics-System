FROM python:3.9-slim

WORKDIR /app

RUN pip install kafka-python

COPY producer.py .

CMD ["python", "producer.py"]
