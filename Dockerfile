FROM python:3.11-slim

USER root

# Install Java (required for PySpark) and utilities
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless curl procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Download PostgreSQL JDBC driver for Spark
RUN mkdir -p /app/jars && \
    curl -L -o /app/jars/postgresql-42.7.1.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.1.jar

# Copy application code
COPY src/ /app/src/
COPY config/ /app/config/
COPY data/input/ /app/data/input/
COPY tests/ /app/tests/
COPY entrypoint.sh /app/entrypoint.sh

# Create output directories
RUN mkdir -p /app/data/output /app/data/reject /app/logs && \
    chmod +x /app/entrypoint.sh

WORKDIR /app

ENV PYTHONPATH=/app
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV SPARK_CLASSPATH=/app/jars/postgresql-42.7.1.jar

ENTRYPOINT ["/app/entrypoint.sh"]
