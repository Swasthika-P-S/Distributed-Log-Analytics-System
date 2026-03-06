FROM apache/spark:3.5.0

USER root

# Official image usually has spark in /opt/spark
WORKDIR /opt/spark/work-dir

# Copy the pre-downloaded JARs from the host
COPY libs /opt/spark/jars/kafka-connector-libs/
COPY spark_job.py /opt/spark/work-dir/

# Ensure spark user has access
RUN chown -R spark:spark /opt/spark/jars/kafka-connector-libs/ && \
    chown -R spark:spark /opt/spark/work-dir

USER spark

# Define the list of JARs to include in the classpath
ENV KAFKA_JARS="/opt/spark/jars/kafka-connector-libs/spark-sql-kafka-0-10_2.12-3.5.0.jar,\
    /opt/spark/jars/kafka-connector-libs/kafka-clients-3.4.1.jar,\
    /opt/spark/jars/kafka-connector-libs/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,\
    /opt/spark/jars/kafka-connector-libs/commons-pool2-2.11.1.jar"

# Run the actual job using the local JARs
CMD ["/opt/spark/bin/spark-submit", \
    "--jars", "/opt/spark/jars/kafka-connector-libs/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/spark/jars/kafka-connector-libs/kafka-clients-3.4.1.jar,/opt/spark/jars/kafka-connector-libs/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/opt/spark/jars/kafka-connector-libs/commons-pool2-2.11.1.jar", \
    "spark_job.py"]
