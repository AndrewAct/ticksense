FROM apache/airflow:2.9.3-python3.11

USER root

# Java is required by PySpark (Spark runs on the JVM).
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    rm -rf /var/lib/apt/lists/*

# Iceberg Spark runtime + AWS bundle — same Iceberg version (1.6.0) as Flink stack.
# Placed in /opt/airflow/jars/ and passed to spark-submit via --jars.
RUN mkdir -p /opt/airflow/jars && \
    curl -fL "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.0/iceberg-spark-runtime-3.5_2.12-1.6.0.jar" \
         -o /opt/airflow/jars/iceberg-spark-runtime.jar && \
    curl -fL "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.0/iceberg-aws-bundle-1.6.0.jar" \
         -o /opt/airflow/jars/iceberg-aws-bundle.jar

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    apache-airflow-providers-apache-spark==4.10.0 \
    apache-airflow-providers-trino==5.7.0 \
    great-expectations==0.18.19 \
    trino==0.328.0 \
    dbt-trino==1.8.0 \
    pandas==2.2.2
