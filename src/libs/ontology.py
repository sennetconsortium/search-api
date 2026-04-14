import os
import sqlite3
from logging import getLogger
from time import sleep

from atlas_consortia_commons.ubkg.ubkg_sdk import UbkgSDK

from libs.http import new_session


# Custom accessors etc. can be added to the Ontology class
class Ontology(UbkgSDK):
    pass


class GeneManager:
    def __init__(self, ubkg_url: str):
        self._session = new_session(ubkg_url)
        self._genes_url = f"{ubkg_url}/genes"
        self._logger = getLogger(__name__)
        db_path = os.path.join(os.path.dirname(__file__), "..", "instance", "gene_cache.db")
        self._db = sqlite3.connect(db_path, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("CREATE TABLE IF NOT EXISTS genes (gene_id TEXT PRIMARY KEY, name TEXT)")
        self._db.commit()

    def get_gene(self, gene_id: str) -> dict:
        # check cache
        row = self._db.execute("SELECT name FROM genes WHERE gene_id = ?", (gene_id,)).fetchone()
        if row:
            self._logger.debug(f"Cache hit for gene_id {gene_id}")
            return {"id": gene_id, "name": row[0]}

        ubkg_gene_id = self._get_gene_id(gene_id)

        # fetch gene data from the ubkg
        res = self._session.get(f"{self._genes_url}/{ubkg_gene_id}", timeout=20)
        res.raise_for_status()
        gene_data = res.json()
        if not isinstance(gene_data, list) or len(gene_data) < 1:
            raise ValueError(f"Gene with id {gene_id} not found in the ubkg")
        gene_data = gene_data[0]

        # update cache
        gene_name = gene_data.get("approved_name", "")
        self._db.execute(
            "INSERT OR REPLACE INTO genes (gene_id, name) VALUES (?, ?)", (gene_id, gene_name)
        )
        self._db.commit()

        return {"id": gene_id, "name": gene_name}

    def get_genes(self, gene_ids: list[str]) -> dict[str, dict]:
        result: dict[str, dict] = {}
        unique_ids = list(set(gene_ids))

        # bulk cache lookup
        placeholders = ",".join("?" * len(unique_ids))
        rows = self._db.execute(
            f"SELECT gene_id, name FROM genes WHERE gene_id IN ({placeholders})", unique_ids
        ).fetchall()
        cached_ids = set()
        for gene_id, name in rows:
            result[gene_id] = {"id": gene_id, "name": name}
            cached_ids.add(gene_id)

        self._logger.debug(f"Cache hit for {len(cached_ids)}/{len(unique_ids)} gene_ids")

        missing = [gene_id for gene_id in unique_ids if gene_id not in cached_ids]

        # split missing gene ids into batches to fetch from ubkg
        batch_size = 50
        ubkg_id_batches = [missing[i : i + batch_size] for i in range(0, len(missing), batch_size)]
        for i, batch in enumerate(ubkg_id_batches):
            ubkg_ids = [self._get_gene_id(gene_id) for gene_id in batch]
            url = f"{self._genes_url}/{",".join(ubkg_ids)}"
            res = self._session.get(url, timeout=20)
            res.raise_for_status()
            if i < len(ubkg_id_batches) - 1:
                sleep(0.2)

            # index batch by ubkg_id for fast lookup when processing response
            ubkg_to_original = {ubkg_id: gene_id for gene_id, ubkg_id in zip(batch, ubkg_ids)}
            inserts = []
            for gene_data in res.json():
                ubkg_id = str(gene_data.get("hgnc_id", ""))
                gene_name = gene_data.get("approved_name", "")
                if ubkg_id and ubkg_id in ubkg_to_original:
                    original_id = ubkg_to_original[ubkg_id]
                    result[original_id] = {"id": original_id, "name": gene_name}
                    inserts.append((original_id, gene_name))
            if inserts:
                self._db.executemany(
                    "INSERT OR REPLACE INTO genes (gene_id, name) VALUES (?, ?)", inserts
                )
                self._db.commit()

        return result

    def clear_cache(self):
        self._db.execute("DELETE FROM genes")
        self._db.commit()

    def _get_gene_id(self, gene_id: str) -> str:
        # "HGNC:12345" or "HGNC:12345 (GENE_SYMBOL)" -> "12345"
        if gene_id.startswith("HGNC:"):
            gene_id = gene_id.split(":")[1].split(" ")[0]
        return gene_id

    def close(self):
        self._session.close()
        self._db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
