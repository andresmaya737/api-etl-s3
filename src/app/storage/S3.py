import awswrangler as wr
import boto3
import logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)


class S3Storage:
    def __init__(self, bucket: str, prefix: str = "", region: str | None = None):
        self.bucket = bucket; self.prefix = prefix.rstrip("/")
        self.s3 = boto3.client("s3", region_name=region)


    def put_parquet(self, df) -> str:
        try:
            now = datetime.utcnow()
            key = f"users/users={now:%Y-%m-%d}/run_ts={now:%H%M%S}.parquet"

            s3_path = f"s3://{self.bucket}/{key}"
            wr.s3.to_parquet(df=df, path=s3_path, index=False)
            log.info("Uploaded Parquet to s3://%s/%s", self.bucket, key)
            return key
        except Exception as exception:
            log.error(f"Error inesperado al escribir Parquet en S3. {exception}")
            raise 