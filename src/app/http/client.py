import requests
import logging, requests, time
from typing import Any, Dict, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from app.config import settings


log = logging.getLogger(__name__)


def _session() -> requests.Session:
    s = requests.Session()
    r = Retry(total=5, backoff_factor=0.5, status_forcelist=[429,500,502,503,504],
              allowed_methods=["GET"])
    a = HTTPAdapter(max_retries=r)
    s.mount("https://", a)

    return s

class ApiClient:
    def __init__(self, base_url: str):
        self.base = base_url
        self.sess = _session()
        self.timeout = settings.http_timeout

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base}/{path.lstrip('/')}"
        log.info("GET %s params=%s", url, params)
        r = self.sess.get(url, params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()