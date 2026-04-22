#!/bin/bash

set -e

# Wait for database to be ready
echo "Waiting for database to be ready..."
sleep 30

# Check if schema already exists
echo "Checking if Hive schema exists..."
if ! /opt/hive/bin/schematool -dbType mysql -info > /dev/null 2>&1; then
    echo "Schema not found. Initializing Hive Metastore schema..."
    /opt/hive/bin/schematool -dbType mysql -initSchema
    echo "Schema initialization completed successfully."
else
    echo "Hive schema already exists. Skipping initialization."
fi

# Start Hive Metastore service
echo "Starting Hive Metastore service..."
exec /opt/hive/bin/hive --service metastore