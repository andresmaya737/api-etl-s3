import boto3, json, gzip, io, logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Union

import awswrangler as wr
import os


log = logging.getLogger(__name__)

class S3Storage:
    def __init__(self, bucket: str, prefix: str = "", region: str | None = None):
        self.bucket = bucket; self.prefix = prefix.rstrip("/")
        self.s3 = boto3.client("s3", region_name=region)


    def put_parquet(self, df) -> str:
    
        now = datetime.utcnow()
        key = f"users/users={now:%Y-%m-%d}/run_ts={now:%H%M%S}.parquet"

        s3_path = f"s3://{self.bucket}/{key}"
        wr.s3.to_parquet(df=df, path=s3_path, index=False)  # partition_cols opcional
        log.info("Uploaded Parquet to s3://%s/%s", self.bucket, key)
        return key
    
