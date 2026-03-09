#!/usr/bin/env python3
"""
Storage Client Module

Provides a unified interface for interacting with cloud storage services
(MinIO, AWS S3, etc.) with support for both object storage and file operations.

Author: Data Engineering Team
"""

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from typing import Dict, List, Optional, Union
from pathlib import Path
import logging
import io
import json
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StorageClient:
    """Unified storage client for MinIO and AWS S3."""

    def __init__(self, config: Dict):
        """
        Initialize storage client with configuration.

        Args:
            config: Dictionary containing storage configuration
                - service_type: 'minio' or 's3' (default: 'minio')
                - endpoint_url: Storage service endpoint URL
                - aws_access_key_id: Access key ID
                - aws_secret_access_key: Secret access key
                - region_name: AWS region name (for S3)
                - bucket_name: Default bucket name
                - secure: Whether to use HTTPS (default: False for MinIO)
        """
        self.service_type = config.get('service_type', 'minio')
        self.bucket_name = config.get('bucket_name', 'default-bucket')
        self.endpoint_url = config.get('endpoint_url', self._get_default_endpoint())
        self.aws_access_key_id = config.get('aws_access_key_id', 'admin')
        self.aws_secret_access_key = config.get('aws_secret_access_key', 'password')
        self.region_name = config.get('region_name', 'us-east-1')
        self.secure = config.get('secure', self.service_type == 's3')

        self.client = self._create_client()

    def _get_default_endpoint(self) -> str:
        """Get default endpoint based on service type."""
        if self.service_type == 'minio':
            return 'http://localhost:9000'
        return 'https://s3.amazonaws.com'

    def _create_client(self):
        """Create storage client based on service type."""
        try:
            if self.service_type == 'minio':
                return boto3.client('s3',
                    endpoint_url=self.endpoint_url,
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                    region_name=self.region_name,
                    use_ssl=self.secure
                )
            elif self.service_type == 's3':
                return boto3.client('s3',
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                    region_name=self.region_name
                )
            else:
                raise ValueError(f"Unsupported service type: {self.service_type}")
        except Exception as e:
            logger.error(f"Error creating storage client: {str(e)}")
            raise

    def upload_object(self, bucket_name: str, key: str, body: Union[str, bytes, io.IOBase],
                     content_type: Optional[str] = None) -> bool:
        """
        Upload an object to storage.

        Args:
            bucket_name: Name of the bucket
            key: Object key (path)
            body: Object content (string, bytes, or file-like object)
            content_type: MIME type of the content

        Returns:
            bool: True if upload successful, False otherwise
        """
        try:
            extra_args = {}
            if content_type:
                extra_args['ContentType'] = content_type

            self.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=body,
                **extra_args
            )
            logger.info(f"Successfully uploaded {key} to {bucket_name}")
            return True
        except (NoCredentialsError, PartialCredentialsError) as e:
            logger.error(f"Credentials error: {str(e)}")
            return False
        except ClientError as e:
            logger.error(f"Client error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error uploading object: {str(e)}")
            return False

    def upload_file(self, bucket_name: str, key: str, file_path: str) -> bool:
        """
        Upload a file to storage.

        Args:
            bucket_name: Name of the bucket
            key: Object key (path)
            file_path: Local file path to upload

        Returns:
            bool: True if upload successful, False otherwise
        """
        try:
            self.client.upload_file(file_path, bucket_name, key)
            logger.info(f"Successfully uploaded file {file_path} to {bucket_name}/{key}")
            return True
        except Exception as e:
            logger.error(f"Error uploading file: {str(e)}")
            return False

    def download_object(self, bucket_name: str, key: str, file_path: str) -> bool:
        """
        Download an object from storage.

        Args:
            bucket_name: Name of the bucket
            key: Object key (path)
            file_path: Local file path to save the object

        Returns:
            bool: True if download successful, False otherwise
        """
        try:
            self.client.download_file(bucket_name, key, file_path)
            logger.info(f"Successfully downloaded {key} from {bucket_name} to {file_path}")
            return True
        except Exception as e:
            logger.error(f"Error downloading object: {str(e)}")
            return False

    def download_object_as_string(self, bucket_name: str, key: str) -> Optional[str]:
        """
        Download an object as a string.

        Args:
            bucket_name: Name of the bucket
            key: Object key (path)

        Returns:
            Optional[str]: Object content as string, or None if error
        """
        try:
            response = self.client.get_object(Bucket=bucket_name, Key=key)
            return response['Body'].read().decode('utf-8')
        except Exception as e:
            logger.error(f"Error downloading object: {str(e)}")
            return None

    def download_object_as_json(self, bucket_name: str, key: str) -> List[dict]:
        """
        Download an object as JSON.

        Args:
            bucket_name: Name of the bucket
            key: Object key (path)

        Returns:
            Optional[dict]: Object content as JSON, or None if error
        """
        try:
            response = self.client.get_object(Bucket=bucket_name, Key=key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except Exception as e:
            logger.error(f"Error downloading object: {str(e)}")
            return None

    def list_objects(self, bucket_name: str, prefix: str = '') -> list:
        """
        List objects in a bucket with optional prefix.

        Args:
            bucket_name: Name of the bucket
            prefix: Optional prefix to filter objects

        Returns:
            list: List of object keys
        """
        try:
            response = self.client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix
            )
            if 'Contents' in response:
                return [item['Key'] for item in response['Contents']]
            return []
        except Exception as e:
            logger.error(f"Error listing objects: {str(e)}")
            return []

    def delete_object(self, bucket_name: str, key: str) -> bool:
        """
        Delete an object from storage.

        Args:
            bucket_name: Name of the bucket
            key: Object key (path)

        Returns:
            bool: True if deletion successful, False otherwise
        """
        try:
            self.client.delete_object(Bucket=bucket_name, Key=key)
            logger.info(f"Successfully deleted {key} from {bucket_name}")
            return True
        except Exception as e:
            logger.error(f"Error deleting object: {str(e)}")
            return False

    def create_bucket(self, bucket_name: str) -> bool:
        """
        Create a new bucket.

        Args:
            bucket_name: Name of the bucket to create

        Returns:
            bool: True if bucket created successfully, False otherwise
        """
        try:
            self.client.create_bucket(Bucket=bucket_name)
            logger.info(f"Successfully created bucket {bucket_name}")
            return True
        except Exception as e:
            logger.error(f"Error creating bucket: {str(e)}")
            return False

    def bucket_exists(self, bucket_name: str) -> bool:
        """
        Check if a bucket exists.

        Args:
            bucket_name: Name of the bucket to check

        Returns:
            bool: True if bucket exists, False otherwise
        """
        try:
            self.client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                return False
            raise
        except Exception as e:
            logger.error(f"Error checking bucket existence: {str(e)}")
            return False


class StorageClientFactory:
    """Factory class for creating storage clients."""

    @staticmethod
    def create_minio_client(config: Dict) -> StorageClient:
        """Create a MinIO client."""
        config['service_type'] = 'minio'
        return StorageClient(config)

    @staticmethod
    def create_s3_client(config: Dict) -> StorageClient:
        """Create an AWS S3 client."""
        config['service_type'] = 's3'
        return StorageClient(config)

    @staticmethod
    def create_client(service_type: str, config: Dict) -> StorageClient:
        """Create a storage client based on service type."""
        config['service_type'] = service_type
        return StorageClient(config)


# Convenience functions for common operations
def upload_csv(bucket_name: str, key: str, data: pd.DataFrame,
              storage_client: Optional[StorageClient] = None,
              **client_config) -> bool:
    """
    Upload a pandas DataFrame as CSV to storage.

    Args:
        bucket_name: Name of the bucket
        key: Object key (path)
        data: DataFrame to upload
        storage_client: Optional existing storage client
        client_config: Configuration for creating a new client

    Returns:
        bool: True if upload successful, False otherwise
    """
    if storage_client is None:
        storage_client = StorageClient(client_config)

    csv_buffer = data.to_csv(index=False, encoding='utf-8')
    return storage_client.upload_object(bucket_name, key, csv_buffer, content_type='text/csv')


def upload_json(bucket_name: str, key: str, data: Union[dict, list],
               storage_client: Optional[StorageClient] = None,
               **client_config) -> bool:
    """
    Upload JSON data to storage.

    Args:
        bucket_name: Name of the bucket
        key: Object key (path)
        data: JSON data (dict or list)
        storage_client: Optional existing storage client
        client_config: Configuration for creating a new client

    Returns:
        bool: True if upload successful, False otherwise
    """
    if storage_client is None:
        storage_client = StorageClient(client_config)

    json_buffer = json.dumps(data)
    return storage_client.upload_object(bucket_name, key, json_buffer, content_type='application/json')


def upload_parquet(bucket_name: str, key: str, data: pd.DataFrame,
                  storage_client: Optional[StorageClient] = None,
                  **client_config) -> bool:
    """
    Upload a pandas DataFrame as Parquet to storage.

    Args:
        bucket_name: Name of the bucket
        key: Object key (path)
        data: DataFrame to upload
        storage_client: Optional existing storage client
        client_config: Configuration for creating a new client

    Returns:
        bool: True if upload successful, False otherwise
    """
    if storage_client is None:
        storage_client = StorageClient(client_config)

    parquet_buffer = data.to_parquet(index=False)
    return storage_client.upload_object(bucket_name, key, parquet_buffer, content_type='application/octet-stream')