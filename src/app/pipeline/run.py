import logging
from typing import Any, Dict, List

from app.config import settings
from app.http.client import ApiClient
from app.storage.S3 import S3Storage

log = logging.getLogger(__name__)


def transforms(items: list[dict]) -> "pd.DataFrame":
    """Transform the data into a Pandas DataFrame."""
    import pandas as pd
    try:
        df = pd.json_normalize(items, sep="__")

        # Ejemplo de transformaciones
        df["full_name"] = df["name__first"] + " " + df["name__last"]
        df["nacionality"] = "col"

        return df
    except Exception as exception:
        log.exception(f"Error while transforming data: {exception}")
        raise


def fetch() -> List[Dict[str, Any]]:
    '''
    Fetch data from the API
    '''
    try:
        client = ApiClient(settings.api_base_url)
        payload = client.get("/api", params={"results": 5000})
        return payload.get("results", [])
    except Exception as exception:
        log.exception(f"Error while fetching data: {exception}")
        raise


def run():
    '''
    Main ETL function
    '''

    #Extract + Transform + Load
    data = fetch()
    df = transforms(data)

    s3 = S3Storage(bucket=settings.s3_bucket, prefix=settings.s3_prefix, region=settings.aws_region)
    key = s3.put_parquet(df)
    
    return key
