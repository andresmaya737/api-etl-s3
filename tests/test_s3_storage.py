import json
import io
import types
import pytest
from botocore.exceptions import ClientError

import app.storage.S3


def test_put_parquet_df_with_wrangler_mock(s3_mock, monkeypatch):
    """Simula awswrangler.s3.to_parquet sin instalar awswrangler/pandas."""
    store = S3Storage(bucket="test-bucket")

    fake_df = object()
    captured = {}

    class FakeWr:
        class s3:
            @staticmethod
            def to_parquet(df, path, index=False, **kwargs):
                captured["df"] = df
                captured["path"] = path
                captured["index"] = index
                captured["kwargs"] = kwargs

    monkeypatch.setitem(globals(), "awswrangler", FakeWr) 
    monkeypatch.setenv("PYTHONPATH", ".")  

    def fake_import_wr(*args, **kwargs):
        return FakeWr
    monkeypatch.setitem(__import__.__globals__, "awswrangler", FakeWr)


    def _fake_import(name, *args, **kwargs):
        if name == "awswrangler":
            return FakeWr
        return __import__(name, *args, **kwargs)
    monkeypatch.setattr("builtins.__import__", _fake_import)

    resp = store.put_parquet("out/data.parquet", fake_df, dataset=True)
    assert resp["Bucket"] == "test-bucket"
    assert resp["Key"] == "out/data.parquet"
    assert resp["Engine"] == "awswrangler"
    assert captured["df"] is fake_df
    assert captured["path"].startswith("s3://test-bucket/")
    assert captured["index"] is False
    assert captured["kwargs"]["dataset"] is True


