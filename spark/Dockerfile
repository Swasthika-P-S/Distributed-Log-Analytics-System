FROM apache/spark:3.5.0

USER root

WORKDIR /opt/spark/work-dir
COPY . /opt/spark/work-dir

# Create ivy cache dir with correct permissions so --packages works
RUN mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars && \
    chown -R spark:spark /home/spark/.ivy2 && \
    chown -R spark:spark /opt/spark/work-dir

USER spark

# Pre-download the Kafka connector JAR into the ivy cache at build time
# This avoids network downloads at container start
RUN /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --help > /dev/null 2>&1 || true

# Run the actual job at container start
CMD ["/opt/spark/bin/spark-submit", \
    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
    "spark_job.py"]
