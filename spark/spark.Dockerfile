FROM apache/spark:3.5.3

USER root

# Install Java and pip + pyspark
RUN apt-get update && apt-get install -y python3-pip openjdk-11-jdk && \
    pip3 install --no-cache-dir pyspark==3.5.3 delta-spark 

# Delta Lake 3.2.0 (Scala 2.12)
RUN curl -L -o /opt/spark/jars/delta-spark_2.12-3.2.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar

# Delta Storage 3.2.0
RUN curl -L -o /opt/spark/jars/delta-storage-3.2.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar

# Hadoop AWS 3.3.4
RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# AWS SDK v1 bundle 1.12.530
RUN curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.530.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.530/aws-java-sdk-bundle-1.12.530.jar

# Configure JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Create Ivy cache
RUN mkdir -p /home/spark/.ivy2/cache && \
    chown -R spark:spark /home/spark/.ivy2

USER spark


# Set working directory
WORKDIR /opt/spark/work-dir

COPY spark/pyproject.toml spark/poetry.lock ./