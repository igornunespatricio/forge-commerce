FROM apache/spark:3.5.3

USER root

# Upgrade pip tooling first (prevents many segfaults)
RUN python3 -m pip install --no-cache-dir --upgrade pip setuptools wheel

# Install Jupyter first
RUN python3 -m pip install --no-cache-dir \
    jupyterlab \
    python-dotenv

# Delta Lake 3.2.0 (Scala 2.12)
RUN curl -L -o /opt/spark/jars/delta-spark_2.12-3.2.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar

# Delta Storage 3.2.0
RUN curl -L -o /opt/spark/jars/delta-storage-3.2.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar

# Install Hadoop S3A dependencies
RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Make PySpark visible to Python
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Workspace
WORKDIR /home/spark

# Copy notebooks
COPY spark/notebooks /home/spark/notebooks

# Create Jupyter runtime directories and fix permissions
RUN mkdir -p /home/spark/.jupyter \
    && mkdir -p /home/spark/.local/share/jupyter \
    && chmod -R 777 /home/spark

# Entrypoint
COPY spark/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

USER root