import os
import logging
from app.log import setup_logging
from app.pipeline.run import run
from app.config import settings

setup_logging(os.getenv("LOG_LEVEL","INFO"))
log = logging.getLogger(__name__)

def handler(event, context):
    log.info('Starting lambda handler')
    log.info("Start → base_url=%s bucket=%s prefix=%s",settings.api_base_url, settings.s3_bucket, settings.s3_prefix)
    
    key = run()

    return {"status": "ok"
            , "bucket": settings.s3_bucket, 
            "s3_key": key
            }
