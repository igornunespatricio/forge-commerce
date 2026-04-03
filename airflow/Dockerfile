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

# Copy entrypoint script and make it executable (while still root)
COPY airflow/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY airflow/requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Install Poetry for Faker data generation
RUN pip install poetry

# Install PySpark for SparkSubmitOperator
RUN pip install pyspark

# Copy DAGs, plugins, and config (will be mounted as volumes in development)
# This is just to ensure the directories exist
COPY airflow/dags/ ${AIRFLOW_HOME}/dags/
COPY airflow/plugins/ ${AIRFLOW_HOME}/plugins/
COPY airflow/config/ ${AIRFLOW_HOME}/config/

ENTRYPOINT ["/entrypoint.sh"]

# Set working directory
WORKDIR ${AIRFLOW_HOME}

# Default command (can be overridden by docker-compose)
CMD ["airflow", "version"]