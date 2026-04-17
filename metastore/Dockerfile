FROM apache/hive:3.1.3

USER root

# Install wget first
RUN apt-get update && \
    apt-get install -y wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install MySQL JDBC driver from Maven Central (correct path)
RUN wget -v https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar -P /opt/hive/lib/ && \
    chmod 644 /opt/hive/lib/mysql-connector-j-8.0.33.jar

# Install Hadoop AWS and AWS SDK for S3A filesystem support
RUN wget -v https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.0/hadoop-aws-3.1.0.jar -P /opt/hive/lib/ && \
    wget -v https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar -P /opt/hive/lib/ && \
    chmod 644 /opt/hive/lib/hadoop-aws-3.1.0.jar && \
    chmod 644 /opt/hive/lib/aws-java-sdk-bundle-1.11.271.jar

# Set environment variables for Hive Metastore
ENV HIVE_HOME /opt/hive
ENV PATH $HIVE_HOME/bin:$PATH

# Copy configuration files
COPY hive-site.xml $HIVE_HOME/conf/hive-site.xml

# Create necessary directories
RUN mkdir -p /tmp/hive && \
    mkdir -p /opt/hive/logs && \
    chmod 777 /tmp/hive && \
    chmod 777 /opt/hive/logs

# Expose Metastore port
EXPOSE 9083

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=10 \
    CMD bash -c 'echo > /dev/tcp/127.0.0.1/9083' || exit 1

# Copy custom startup script
COPY start-metastore.sh /opt/hive/bin/start-metastore.sh
RUN chmod +x /opt/hive/bin/start-metastore.sh

# Override default entrypoint to prevent automatic schema initialization
ENTRYPOINT []

# Default command - use our custom startup script
CMD ["/opt/hive/bin/start-metastore.sh"]
