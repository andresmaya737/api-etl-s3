from typing import Any, Dict, List
import logging
from app.http.client import ApiClient
from app.storage.S3 import S3Storage
from app.config import settings


log = logging.getLogger(__name__)

def transforms(items: list[dict]) -> "pd.DataFrame":
    ''' 
    Transform the data
    '''
    import pandas as pd
    df = pd.json_normalize(items, sep="__")   

    #some example transformations creating new columns to test crawler
    df['full_name'] = df['name__first'] + ' ' + df['name__last']
    df['nacionality'] = 'col'
    return df

def fetch() -> List[Dict[str, Any]]:
    '''
    Fetch data from the API
    '''
    client = ApiClient(settings.api_base_url)
    payload = client.get("/api", params={"results": 5000})
    return payload.get("results", [])

def run():
    '''
    Main ETL function
    '''
    data = fetch()

    df = transforms(data)

    s3 = S3Storage(bucket=settings.s3_bucket, prefix=settings.s3_prefix, region=settings.aws_region)
    key = s3.put_parquet(df)
    

    return key

    
    
