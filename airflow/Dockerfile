# Airflow Dockerfile for Forge Commerce
# Based on official Apache Airflow image

FROM apache/airflow:3.1.8

# Switch to root to install system dependencies
USER root

# Install system dependencies needed for some Python packages
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    gcc \
    python3-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Install Poetry for Faker data generation
RUN pip install poetry

# Install PySpark for SparkSubmitOperator
RUN pip install pyspark

# Copy DAGs, plugins, and config (will be mounted as volumes in development)
# This is just to ensure the directories exist
COPY dags/ ${AIRFLOW_HOME}/dags/
COPY plugins/ ${AIRFLOW_HOME}/plugins/
COPY config/ ${AIRFLOW_HOME}/config/

# Set proper ownership
USER root
RUN chown -R airflow:airflow ${AIRFLOW_HOME}
USER airflow

# Set working directory
WORKDIR ${AIRFLOW_HOME}

# Default command (can be overridden by docker-compose)
CMD ["airflow", "version"]