from dotenv import load_dotenv
import os

load_dotenv()


print(os.getenv("BUCKET_NAME"))
print(os.getenv("AWS_S3_ENDPOINT"))
print(os.getenv("AWS_SECRET_ACCESS_KEY"))
print(os.getenv("AWS_ACCESS_KEY_ID"))
print(os.getenv("AWS_S3_REGION"))