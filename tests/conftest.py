import os
import pytest
from moto import mock_aws
import boto3

@pytest.fixture(autouse=True)
def _set_env(monkeypatch):
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("S3_BUCKET", "test-bucket")
    monkeypatch.setenv("S3_PREFIX", "ingesta/randomuser/")
    monkeypatch.setenv("API_BASE_URL", "https://randomuser.me")
    monkeypatch.setenv("HTTP_TIMEOUT", "5")
    yield

@pytest.fixture
def s3_mock():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")
        yield s3
