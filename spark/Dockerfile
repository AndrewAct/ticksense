FROM bitnami/spark:3.5

USER root

# Iceberg Spark runtime (Spark 3.5 / Scala 2.12) + AWS bundle for S3FileIO.
# Same Iceberg 1.6.0 as the Flink and Airflow images — consistent catalog format.
RUN curl -fL \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.0/iceberg-spark-runtime-3.5_2.12-1.6.0.jar" \
    -o /opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.0.jar && \
    curl -fL \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.0/iceberg-aws-bundle-1.6.0.jar" \
    -o /opt/bitnami/spark/jars/iceberg-aws-bundle-1.6.0.jar

COPY jobs/ /opt/spark/jobs/

USER 1001
