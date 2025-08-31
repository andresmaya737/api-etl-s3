import json
import types
import pytest
from freezegun import freeze_time

from app.pipeline.run import run

class DummyResp:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status
    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError("HTTP error")
    def json(self):
        return self._payload

@pytest.fixture
def sample_randomuser():
    return {
        "results": [
            {
                "name": {"first": "Ada", "last": "Lovelace"},
                "location": {"city": "London"},
                "email": "ada@example.com"
            }
        ],
        "info": {"seed": "abc", "results": 1}
    }

def test_run_wrangler_path(monkeypatch, sample_randomuser):
    def fake_get(url, timeout):
        assert "randomuser.me/api/?results=1" in url
        return DummyResp(sample_randomuser)

    monkeypatch.setattr("requests.get", fake_get)

    # Mock awswrangler and pandas.json_normalize
    calls = {}
    class FakePandas:
        @staticmethod
        def json_normalize(items, sep="__"):
            # devolvemos un objeto "df" trivial
            return {"_kind": "df", "rows": items, "sep": sep}

    class FakeWrS3:
        @staticmethod
        def to_parquet(df, path, index=False, **kw):
            calls["path"] = path
            calls["df"] = df
            calls["index"] = index
            calls["kw"] = kw

    class FakeWr:
        s3 = FakeWrS3

    def _fake_import(name, *args, **kwargs):
        if name == "pandas":
            return FakePandas
        if name == "awswrangler":
            return FakeWr
        return __import__(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", _fake_import)

    with freeze_time("2025-08-29 03:04:05"):
        res = run()

    assert res["status"] == "ok"
    assert res["bucket"] == "test-bucket"
    assert res["rows"] == 1
    assert "random_user=2025-01-02" in res["s3_key"]

    assert calls["df"]["_kind"] == "df"
    assert calls["path"].startswith("s3://test-bucket/")
