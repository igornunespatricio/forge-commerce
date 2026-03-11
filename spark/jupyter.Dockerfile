FROM jupyter/pyspark-notebook:x86_64-ubuntu-22.04

# Install additional Python packages
USER root
RUN pip install --no-cache-dir python-dotenv boto3

# Copy tools directory to make it available in the container
# COPY ../tools /home/jovyan/tools

# Set proper permissions for the tools directory
# RUN chown -R jovyan:users /home/jovyan/tools

# Switch back to jovyan user
USER jovyan
