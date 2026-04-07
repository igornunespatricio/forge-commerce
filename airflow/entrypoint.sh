#!/bin/bash
set -e

# Ensure required directories exist with proper permissions
mkdir -p ${AIRFLOW_HOME}/logs
mkdir -p ${AIRFLOW_HOME}/dags
mkdir -p ${AIRFLOW_HOME}/plugins
mkdir -p ${AIRFLOW_HOME}/config

# Fix permissions if running as root (for docker-compose user 0:0)
if [ "$(id -u)" = "0" ]; then
    # Get the AIRFLOW_UID from environment or use default
    AIRFLOW_UID=${AIRFLOW_UID:-50000}
    
    # Change ownership of Airflow home
    chown -R ${AIRFLOW_UID}:0 ${AIRFLOW_HOME}
fi

# Execute the main command
exec airflow "$@"
