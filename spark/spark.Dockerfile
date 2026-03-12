FROM apache/spark:4.0.2-scala2.13-java17-python3-r-ubuntu

# Set working directory
WORKDIR /opt/spark/work-dir

# Download Hadoop AWS and AWS SDK v1 JARs for S3/MinIO support
RUN wget -O /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN wget -O /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Set proper permissions
RUN chmod 644 /opt/spark/jars/hadoop-aws-3.3.4.jar \
    /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar


# FROM apache/spark-py:v3.4.0