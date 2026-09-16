"""Construction of the reusable T-Invest HTTP transport session."""

import requests
from requests.adapters import HTTPAdapter


def build_http_session(*, pool_connections: int, pool_maxsize: int) -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
        max_retries=0,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
