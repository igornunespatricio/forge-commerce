# MinIO Docker Container
# Based on Alpine Linux for minimal size
FROM alpine:latest

# Install MinIO and required packages
RUN apk add --no-cache \
    ca-certificates \
    curl \
    && rm -rf /var/cache/apk/*

# Download and install MinIO
RUN curl -LO https://dl.min.io/server/minio/release/linux-amd64/minio \
    && chmod +x minio \
    && mv minio /usr/local/bin/

# Create data directory
RUN mkdir -p /data/minio

# Expose MinIO ports
EXPOSE 9000

# Health check
HEALTHCHECK --interval=30s --timeout=20s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:9000/minio/health/live || exit 1

# Default command
CMD ["/usr/local/bin/minio", "server", "/data/minio", "--console-address", ":9001"]

# Metadata
LABEL maintainer="Forge Commerce Team" \
    version="1.0" \
    description="MinIO Object Storage Server"