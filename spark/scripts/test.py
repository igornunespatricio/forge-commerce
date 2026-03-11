from dotenv import load_dotenv
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))
from tools.storage_client import StorageClient

load_dotenv()


print(os.getenv("BUCKET_NAME"))
print(os.getenv("AWS_S3_ENDPOINT"))
print(os.getenv("AWS_SECRET_ACCESS_KEY"))
print(os.getenv("AWS_ACCESS_KEY_ID"))
print(os.getenv("AWS_S3_REGION"))


config = {
    "service_type": os.getenv("SERVICE_TYPE", "minio"),
    "endpoint_url": os.getenv("AWS_S3_ENDPOINT"),
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "region_name": os.getenv("AWS_S3_REGION"),
    "bucket_name": os.getenv("BUCKET_NAME"),
}

client = StorageClient(config)

print(client.aws_access_key_id)
print(client.aws_secret_access_key)
print(client.region_name)
print(client.bucket_name)
print(client.endpoint_url)
print(client.secure)
print(client.service_type)


print(client.list_objects("raw", "customers"))
