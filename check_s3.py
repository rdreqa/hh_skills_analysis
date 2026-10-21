import os

import boto3
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="us-east-1",
)

bucket = os.getenv("S3_BUCKET")
object_key = os.getenv("S3_OBJECT")

print(f"Bucket: {bucket}")

response = s3.list_objects_v2(Bucket=bucket)

for obj in response.get("Contents", []):
    print(
        f"File: {obj['Key']} | "
        f"Size: {obj['Size'] / 1024**2:.2f} MiB"
    )

metadata = s3.head_object(
    Bucket=bucket,
    Key=object_key,
)

print("\nDataset found!")
print(f"Object: {object_key}")
print(f"Size: {metadata['ContentLength'] / 1024**2:.2f} MiB")

