FROM apache/spark:3.5.3

USER root

# Install Python dependencies
RUN python3 -m pip install --no-cache-dir \
    jupyterlab \
    python-dotenv

# Install Hadoop S3A dependencies
RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Ensure Spark Python libraries are visible
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

# Create workspace
RUN mkdir -p /home/spark/tools

WORKDIR /home/spark

# Copy project tools
COPY ../tools /home/spark/tools

# Fix permissions
RUN chown -R spark:spark /home/spark

# Switch back to Spark runtime user
USER spark