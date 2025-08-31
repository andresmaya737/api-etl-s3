from pydantic import BaseSettings

class Settings(BaseSettings):
    api_base_url: str = "https://randomuser.me"
    api_key: str = "dummy"
    s3_bucket: str = 'test-bucket'
    s3_prefix: str = "users/"
    aws_region: str = "us-east-1"
    http_timeout: int = 15
    log_level: str = "INFO"

settings = Settings()