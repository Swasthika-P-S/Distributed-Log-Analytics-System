FROM python:3.9-slim

LABEL description="Spark Batch Log Analyser"

WORKDIR /app

# Java is required by PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyspark==3.5.4

COPY spark_job.py .

CMD ["python", "-u", "spark_job.py"]
