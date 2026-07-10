from time import sleep
from typing import Any, Iterator

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry


class SenotypeAPIService:
    def __init__(self, base_url: str):
        self._base_url = base_url.rstrip("/")

        self._session = Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[408, 429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)
        self._session.mount(self._base_url, adapter)

    def __enter__(self) -> "SenotypeAPIService":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def close(self) -> None:
        self._session.close()

    def get_senotype(self, uuid: str, token: str) -> dict[str, Any] | None:
        """Retrieve a single senotype document by uuid, or None if it does not exist."""
        headers = {"Authorization": f"Bearer {token}"}
        res = self._session.get(f"{self._base_url}/senotypes/{uuid}", headers=headers)
        if res.status_code == 404:
            return None
        res.raise_for_status()
        data = res.json()
        if "senotype" not in data:
            raise ValueError(f"Unexpected response format from senotype-api: {data}")
        return data["senotype"]

    def iter_senotypes(self, token: str, chunk_size: int = 50) -> Iterator[list[dict[str, Any]]]:
        """Page through the /senotypes list endpoint, yielding one chunk at a time.

        This avoids holding every senotype document in memory at once - each
        chunk is fetched, yielded for processing, then discarded.
        """
        headers = {"Authorization": f"Bearer {token}"}

        offset = 0
        while True:
            res = self._session.get(
                f"{self._base_url}/senotypes",
                headers=headers,
                params={"limit": chunk_size, "offset": offset, "order": "asc"},
            )
            res.raise_for_status()
            payload = res.json()
            results = payload.get("senotypes", [])
            if not results:
                return

            yield results

            pagination = payload.get("pagination", {})
            total = pagination.get("total", offset + len(results))
            offset += len(results)
            if len(results) < chunk_size or offset >= total:
                return
            else:
                sleep(0.1)
