# MinIO Docker Container

This directory contains a Docker setup for MinIO object storage server.

## Quick Start

To build and run the MinIO container:

```bash
cd minio
docker-compose up -d
```

This will:
- Build the MinIO image from the Dockerfile
- Start the MinIO server on ports 9000 (API) and 9001 (console)
- Create a default bucket called `forge-commerce`
- Create a user `forge-commerce-user` with read/write permissions

## Access Information

- **Console**: http://localhost:9001
- **API Endpoint**: http://localhost:9000
- **Default Admin Credentials**:
  - Username: `admin`
  - Password: `password`
- **Forge Commerce User**:
  - Username: `forge-commerce-user`
  - Password: `forge-commerce-pass`

## Manual Build and Run

If you prefer to build and run manually:

```bash
# Build the image
docker build -t minio-forge-commerce .

# Run the container
docker run -d --name minio-forge-commerce \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=admin \
  -e MINIO_ROOT_PASSWORD=password \
  -v minio_data:/data/minio \
  minio-forge-commerce
```

## Data Persistence

Data is persisted in a Docker volume named `minio_data`. To remove the volume and all data:

```bash
docker-compose down -v
```

## Development

The MinIO container is configured for development use. For production deployments, consider:
- Using stronger passwords
- Configuring TLS/SSL
- Setting up proper access controls
- Using a dedicated storage backend

## Troubleshooting

- **Container won't start**: Check if port 9000 or 9001 is already in use
- **Health check failing**: MinIO might still be starting up, wait a few seconds
- **Data not persisting**: Ensure the volume is properly mounted