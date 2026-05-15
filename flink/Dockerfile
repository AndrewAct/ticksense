# Stage 1: pull JDK headers only.
# flink:1.18 is built on eclipse-temurin JRE (no include/ dir), but PyFlink's
# pemja C extension needs jni.h at JAVA_HOME/include to compile.
FROM eclipse-temurin:11-jdk-jammy AS jdk-headers

# Stage 2: actual Flink image.
FROM flink:1.18-scala_2.12-java11

# Graft JDK include headers into the JRE path so pemja can find jni.h.
COPY --from=jdk-headers /opt/java/openjdk/include /opt/java/openjdk/include

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev \
        build-essential \
        curl ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    ln -s /usr/bin/python3 /usr/bin/python

RUN pip3 install --no-cache-dir apache-flink==1.18.0

# ── Connector JARs ────────────────────────────────────────────────────────────

# Kafka connector (Flink 1.18)
RUN curl -fL \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar" \
    -o /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar

# Iceberg Flink runtime (matches iceberg-rest:1.6.0)
RUN curl -fL \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/1.6.0/iceberg-flink-runtime-1.18-1.6.0.jar" \
    -o /opt/flink/lib/iceberg-flink-runtime-1.18-1.6.0.jar

# Iceberg AWS bundle (S3FileIO + AWS SDK v2)
RUN curl -fL \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.0/iceberg-aws-bundle-1.6.0.jar" \
    -o /opt/flink/lib/iceberg-aws-bundle-1.6.0.jar

# Flink-shaded Hadoop — provides org.apache.hadoop.conf.Configuration needed
# by iceberg-flink FlinkCatalogFactory without requiring a full Hadoop install.
RUN curl -fL \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar" \
    -o /opt/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar

# ── S3 plugin for Flink checkpoint storage ────────────────────────────────────
RUN mkdir -p /opt/flink/plugins/s3-fs-presto && \
    cp /opt/flink/opt/flink-s3-fs-presto-*.jar \
       /opt/flink/plugins/s3-fs-presto/

# ── Jobs ─────────────────────────────────────────────────────────────────────
COPY jobs/ /opt/flink/jobs/

ENV PYTHONPATH=/opt/flink/jobs
