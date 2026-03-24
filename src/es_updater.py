import json
import threading
from typing import Dict, List, Optional

from requests import Session


class ESBulkUpdater:
    """
    Bulk updater for Elasticsearch using the _bulk endpoint.

    This class allows you to queue create, update, and delete operations and sends them in bulk
    to Elasticsearch. It is designed to be used as a context manager, which ensures that any
    remaining operations are flushed when exiting the context.

    Examples
    --------
    Using as a context manager:

    from es_updater import ESBulkUpdater
    with ESBulkUpdater(es_url="http://localhost:9200", index="my-index") as updater:
        updater.add_create("doc1", {"field": "value"})
        updater.add_update("doc2", {"field": "new value"}, upsert=True)
        updater.add_delete("doc3")
        # All operations are sent automatically when the block exits
    """

    def __init__(
        self,
        es_url: str,
        index: str,
        max_bytes: int = 20 * 1024 * 1024,
        session: Optional[Session] = None,
    ):
        """
        Initialize the ESBulkUpdater.

        Parameters
        ----------
        es_url : str
            Base URL to Elasticsearch (e.g., 'http://localhost:9200').
        index : str
            Name of the index to update.
        max_bytes : int, optional
            Maximum payload size in bytes before sending (default is 20MB).
        session : Optional[Session], optional
            Optional requests.Session (if not provided, a new one is created).
        """
        self.es_url = es_url.rstrip("/")
        self.index = index
        self.max_bytes = max_bytes
        self.session = session or Session()
        self._lock = threading.Lock()
        self._payload_lines: List[str] = []
        self._payload_bytes = 0
        self._errored_ids: List[str] = []

    def add_create(self, doc_id: str, doc: Dict):
        """
        Queue a create operation for a document (fails if already exists).

        Parameters
        ----------
        doc_id : str
            Document ID to create.
        doc : dict
            Document content to index.
        """
        action = {"create": {"_id": doc_id}}
        action_line = json.dumps(action, separators=(",", ":"))
        doc_line = json.dumps({"doc": doc}, separators=(",", ":"))
        with self._lock:
            self._payload_lines.append(action_line)
            self._payload_lines.append(doc_line)
            self._payload_bytes += (
                len(action_line.encode("utf-8")) + len(doc_line.encode("utf-8")) + 2
            )  # +2 for newlines
            if self._payload_bytes >= self.max_bytes:
                self.send()

    def add_creates(self, docs: Dict[str, Dict]):
        """
        Queue multiple create operations for documents (fails if already exists).

        Parameters
        ----------
        docs : dict of str to dict
            A dictionary mapping doc_id to document content.
        """
        with self._lock:
            for doc_id, doc in docs.items():
                action = {"create": {"_id": doc_id}}
                action_line = json.dumps(action, separators=(",", ":"))
                doc_line = json.dumps({"doc": doc}, separators=(",", ":"))
                self._payload_lines.append(action_line)
                self._payload_lines.append(doc_line)
                self._payload_bytes += (
                    len(action_line.encode("utf-8")) + len(doc_line.encode("utf-8")) + 2
                )  # +2 for newlines
            if self._payload_bytes >= self.max_bytes:
                self.send()

    def add_update(self, doc_id: str, doc: Dict, upsert: bool = False):
        """
        Queue an update (index) operation for a document.

        Parameters
        ----------
        doc_id : str
            Document ID to update.
        doc : dict
            Document content to update.
        upsert : bool, optional
            If True, insert the document if it does not exist (default is False).
        """
        action = {"update": {"_id": doc_id}}
        action_line = json.dumps(action, separators=(",", ":"))
        doc_line = json.dumps({"doc": doc, "doc_as_upsert": upsert}, separators=(",", ":"))
        with self._lock:
            self._payload_lines.append(action_line)
            self._payload_lines.append(doc_line)
            self._payload_bytes += (
                len(action_line.encode("utf-8")) + len(doc_line.encode("utf-8")) + 2
            )  # +2 for newlines
            if self._payload_bytes >= self.max_bytes:
                self.send()

    def add_updates(self, docs: Dict[str, Dict], upsert: bool = False):
        """
        Queue multiple update (index) operations for documents.

        Parameters
        ----------
        docs : dict of str to dict
            A dictionary mapping doc_id to document content.
        upsert : bool, optional
            If True, insert the document if it does not exist (default is False).
        """
        with self._lock:
            for doc_id, doc in docs.items():
                action = {"update": {"_id": doc_id}}
                action_line = json.dumps(action, separators=(",", ":"))
                doc_line = json.dumps({"doc": doc, "doc_as_upsert": upsert}, separators=(",", ":"))
                self._payload_lines.append(action_line)
                self._payload_lines.append(doc_line)
                self._payload_bytes += (
                    len(action_line.encode("utf-8")) + len(doc_line.encode("utf-8")) + 2
                )  # +2 for newlines
            if self._payload_bytes >= self.max_bytes:
                self.send()

    def add_delete(self, doc_id: str):
        """
        Queue a delete operation for a document by ID.

        Parameters
        ----------
        doc_id : str
            Document ID to delete.
        """
        action = {"delete": {"_id": doc_id}}
        action_line = json.dumps(action, separators=(",", ":"))
        with self._lock:
            self._payload_lines.append(action_line)
            self._payload_bytes += len(action_line.encode("utf-8")) + 1  # +1 for newline
            if self._payload_bytes >= self.max_bytes:
                self.send()

    def add_deletes(self, doc_ids: List[str]):
        """
        Queue multiple delete operations for documents by ID.

        Parameters
        ----------
        doc_ids : list of str
            List of document IDs to delete.
        """
        with self._lock:
            for doc_id in doc_ids:
                action = {"delete": {"_id": doc_id}}
                action_line = json.dumps(action, separators=(",", ":"))
                self._payload_lines.append(action_line)
                self._payload_bytes += len(action_line.encode("utf-8")) + 1  # +1 for newline
            if self._payload_bytes >= self.max_bytes:
                self.send()

    def send(self):
        """
        Send the accumulated bulk request to Elasticsearch and log failed _ids.

        Raises
        ------
        Exception
            If the request fails or Elasticsearch returns an error.
        """
        with self._lock:
            if not self._payload_lines:
                return
            data = "\n".join(self._payload_lines) + "\n"
            url = f"{self.es_url}/{self.index}/_bulk"
            headers = {"Content-Type": "application/x-ndjson"}
            try:
                response = self.session.post(url, data=data, headers=headers)
                response.raise_for_status()

                # A 200 status code was returned, but we still need to check for errors
                resp_json = response.json()
                if resp_json.get("errors"):
                    for item in resp_json.get("items", []):
                        for op, result in item.items():
                            if result.get("error"):
                                failed_id = result.get("_id")
                                self._errored_ids.append(failed_id)
            except Exception:
                # On any error, return all _ids in the current payload as failed
                for line in self._payload_lines:
                    try:
                        obj = json.loads(line)
                        # Look for _id in any action line
                        for op, v in obj.items():
                            if isinstance(v, dict) and "_id" in v:
                                self._errored_ids.append(v["_id"])
                    except Exception:
                        continue
                raise
            finally:
                self._payload_lines.clear()
                self._payload_bytes = 0

    def flush(self):
        """
        Send any remaining operations.
        """
        self.send()

    @property
    def errored_ids(self) -> List[str]:
        """
        Get the list of document IDs that failed to update.

        Returns
        -------
        list of str
            List of document IDs that failed to update.
        """
        return self._errored_ids

    def __enter__(self):
        """
        Enter the runtime context related to this object.

        Returns
        -------
        ESBulkUpdater
            The instance itself.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the runtime context and flush any remaining operations.

        Parameters
        ----------
        exc_type : type
            Exception type (if any).
        exc_val : Exception
            Exception value (if any).
        exc_tb : traceback
            Exception traceback (if any).

        Returns
        -------
        bool
            False to indicate exceptions are not suppressed.
        """
        self.flush()
        return False
