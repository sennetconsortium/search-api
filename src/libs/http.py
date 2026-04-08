from typing import List, Union

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry


def new_session(prefix: Union[str, List[str]]):
    session = Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    if isinstance(prefix, str):
        prefix = [prefix]

    for p in prefix:
        session.mount(p, HTTPAdapter(max_retries=retries))

    return session
