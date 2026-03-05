FROM apache/spark:3.5.0

USER root

# Official image usually has spark in /opt/spark
# Change workdir to /opt/spark/work-dir for the official image
WORKDIR /opt/spark/work-dir
COPY . /opt/spark/work-dir

# Set permissions
RUN chown -R spark:spark /opt/spark/work-dir

USER spark

# Use absolute path for spark-submit and match package version to Spark version
CMD ["/opt/spark/bin/spark-submit", \
    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
    "spark_job.py"]
