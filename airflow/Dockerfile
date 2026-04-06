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

# Install Java for Spark-submit
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

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

# Install Faker dependencies
# COPY faker/pyproject.toml /tmp/faker/pyproject.toml
# COPY faker/poetry.lock /tmp/faker/poetry.lock
# RUN cd /tmp/faker && poetry install --no-root --only main

# Install PySpark and delta-spark for SparkSubmitOperator
RUN pip install pyspark==3.5.3 delta-spark


RUN curl -L -o /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars/aws-java-sdk-bundle-1.12.530.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.530/aws-java-sdk-bundle-1.12.530.jar

# Delta Lake JARs (matching PySpark 3.5.3 / Hadoop 3.3.4)
RUN curl -L -o /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars/delta-spark_2.12-3.2.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
    curl -L -o /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars/delta-storage-3.2.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar


# Set SPARK_HOME to pyspark's installation directory
# ENV SPARK_HOME=/home/airflow/.local/lib/python3.12/site-packages/pyspark
# ENV PATH=$SPARK_HOME/bin:$PATH

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