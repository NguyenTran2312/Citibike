import os

import boto3
from botocore.config import Config


def upload_raw_json(raw_path, feed_name, poll_id, collected_at):
    bucket = os.getenv("RAW_BUCKET", "raw")
    key = f"gbfs/{feed_name}/date={collected_at:%Y-%m-%d}/{poll_id}/raw.json"

    client = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            connect_timeout=5,
            read_timeout=15,
            retries={"mode": "standard", "total_max_attempts": 3},
        ),
    )

    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=raw_path.read_bytes(),
            ContentType="application/json",
        )
    finally:
        client.close()

    return f"s3://{bucket}/{key}"
