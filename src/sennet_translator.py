import datetime
import decimal
import hashlib
import json
import logging
import math
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from atlas_consortia_commons.object import enum_val
from atlas_consortia_commons.ubkg import initialize_ubkg
from flask import Config, Flask, Response
from hubmap_commons.hm_auth import AuthHelper
from hubmap_commons.S3_worker import S3Worker
from indexer import Indexer
from requests import RequestException, Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from yaml import safe_load

from es_updater import ESBulkUpdater
from libs.memcached_progress import MemcachedWriteProgress, create_memcached_client
from libs.ontology import Ontology

if "search-adaptor/src" not in sys.path:
    sys.path.append("search-adaptor/src")

from opensearch_helper_functions import BulkUpdate
from translator.tranlation_helper_functions import get_all_reindex_enabled_indice_names
from translator.translator_interface import TranslatorInterface

logging.basicConfig(
    format="[%(asctime)s] %(levelname)s in %(module)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

TIMEOUT = 20


@dataclass(frozen=True)
class Index:
    name: str
    url: str
    public: str
    private: str


class Translator(TranslatorInterface):
    ACCESS_LEVEL_PUBLIC = "public"
    ACCESS_LEVEL_CONSORTIUM = "consortium"
    DATASET_STATUS_PUBLISHED = "published"
    DEFAULT_INDEX_WITHOUT_PREFIX = ""
    INDICES = {}
    TRANSFORMERS = {}  # Not used in SenNet
    DEFAULT_ENTITY_API_URL = ""

    failed_entity_api_calls = []
    failed_entity_ids = []
    skip_comparison = False
    transformation_resources = {}  # Not used in SenNet

    def __init__(self, config, ubkg_instance, token):
        try:
            self.indices: dict = {}
            self.self_managed_indices: dict = {}
            indices = config["INDICES"]

            # Do not include the indexes that are self managed
            for key, value in indices["indices"].items():
                if "reindex_enabled" in value and value["reindex_enabled"] is True:
                    self.indices[key] = value
                else:
                    self.self_managed_indices[key] = value

            self.DEFAULT_INDEX_WITHOUT_PREFIX: str = indices["default_index"]
            self.INDICES: dict = {
                "default_index": self.DEFAULT_INDEX_WITHOUT_PREFIX,
                "indices": self.indices,
            }
            self.DEFAULT_ENTITY_API_URL = self.INDICES["indices"][
                self.DEFAULT_INDEX_WITHOUT_PREFIX
            ]["document_source_endpoint"].strip("/")

            self.indexer = Indexer(self.indices, self.DEFAULT_INDEX_WITHOUT_PREFIX)
            self.ubkg_instance = ubkg_instance
            Ontology.set_instance(self.ubkg_instance)

            self.entity_types = Ontology.ops(as_arr=True, cb=enum_val).entities()
            self.entities = Ontology.ops().entities()

            self.index_config = [
                Index(
                    name=index,
                    url=cfg["elasticsearch"]["url"],
                    public=cfg["public"],
                    private=cfg["private"],
                )
                for index, cfg in self.indices.items()
            ]

            # Keep a dictionary of each ElasticSearch index in an index group which may be
            # looked up for the re-indexing process.
            self.index_group_es_indices = {
                "entities": {
                    "public": f"{self.INDICES['indices']['entities']['public']}",
                    "private": f"{self.INDICES['indices']['entities']['private']}",
                }
            }

            logger.debug("=========== INDICES config ===========")
            logger.debug(self.INDICES)

        except Exception as e:
            raise ValueError(f"Invalid indices config: {e}")

        self.app_client_id = config["APP_CLIENT_ID"]
        self.app_client_secret = config["APP_CLIENT_SECRET"]
        self.memcached_server = config["MEMCACHED_SERVER"] if config["MEMCACHED_MODE"] else None
        self.memcached_prefix = config["MEMCACHED_PREFIX"]
        self.token = token

        self.request_headers = {"Authorization": f"Bearer {token}"}
        self.entity_api_url = self.indices[self.DEFAULT_INDEX_WITHOUT_PREFIX][
            "document_source_endpoint"
        ].strip("/")

        # Add index_version by parsing the VERSION file
        self.index_version = (
            (Path(__file__).absolute().parent.parent / "VERSION").read_text()
        ).strip()

        self.bulk_update_size = indices.get("bulk_update_size", 100)

    # Public methods

    def init_auth_helper(self):
        if AuthHelper.isInitialized() is False:
            auth_helper = AuthHelper.create(self.app_client_id, self.app_client_secret)
        else:
            auth_helper = AuthHelper.instance()

        return auth_helper

    def delete_and_recreate_indices(self, specific_index=None):
        try:
            logger.info("Start executing delete_and_recreate_indices()")
            public_index = None
            private_index = None

            # Delete and recreate target indices
            if specific_index:
                public_index = self.INDICES["indices"][specific_index]["public"]
                private_index = self.INDICES["indices"][specific_index]["private"]

                try:
                    self.indexer.delete_index(public_index)
                    self.indexer.delete_index(private_index)
                except Exception as e:
                    logging.error(
                        f"Error deleting indices: {public_index} and {private_index}: {e}",
                    )

                index_mapping_file = self.INDICES["indices"][specific_index]["elasticsearch"][
                    "mappings"
                ]

                # read the elasticserach specific mappings
                index_mapping_settings = safe_load(
                    (Path(__file__).absolute().parent / index_mapping_file).read_text()
                )

                self.indexer.create_index(public_index, index_mapping_settings)
                self.indexer.create_index(private_index, index_mapping_settings)

                logger.info("Finished executing delete_and_recreate_indices()")

            else:
                for index in self.indices.keys():
                    # each index should have a public/private index
                    public_index = self.INDICES["indices"][index]["public"]
                    private_index = self.INDICES["indices"][index]["private"]

                    try:
                        self.indexer.delete_index(public_index)
                        self.indexer.delete_index(private_index)
                    except Exception as e:
                        logging.error(
                            f"Error deleting indices: {public_index} and {private_index}: {e}",
                        )

                    # get the specific mapping file for the designated index
                    index_mapping_file = self.INDICES["indices"][index]["elasticsearch"]["mappings"]

                    # read the elasticserach specific mappings
                    index_mapping_settings = safe_load(
                        (Path(__file__).absolute().parent / index_mapping_file).read_text()
                    )

                    self.indexer.create_index(public_index, index_mapping_settings)
                    self.indexer.create_index(private_index, index_mapping_settings)

                    logger.info("Finished executing delete_and_recreate_indices()")

        except Exception:
            msg = "Exception encountered during executing delete_and_recreate_indices()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    # TranslatorInterface methods

    def translate_all(self):
        with self._new_session() as session:
            progress_writer = None
            try:
                logger.info("############# Reindex Live Started #############")
                start = time.time()
                delete_failure_results = {}

                # Make calls to entity-api to get a list of uuids for entity types
                source_uuids_list = self._call_entity_api(
                    endpoint_base="source",
                    endpoint_suffix="entities",
                    url_property="uuid",
                    session=session,
                )
                sample_uuids_list = self._call_entity_api(
                    endpoint_base="sample",
                    endpoint_suffix="entities",
                    url_property="uuid",
                    session=session,
                )
                dataset_uuids_list = self._call_entity_api(
                    endpoint_base="dataset",
                    endpoint_suffix="entities",
                    url_property="uuid",
                    session=session,
                )
                upload_uuids_list = self._call_entity_api(
                    endpoint_base="upload",
                    endpoint_suffix="entities",
                    url_property="uuid",
                    session=session,
                )
                collection_uuids_list = self._call_entity_api(
                    endpoint_base="collection",
                    endpoint_suffix="entities",
                    url_property="uuid",
                    session=session,
                )

                # Merge into a big set with no duplicates
                all_entities_uuids = set(
                    source_uuids_list
                    + sample_uuids_list
                    + dataset_uuids_list
                    + upload_uuids_list
                    + collection_uuids_list
                )

                # Only need this comparison for the live /reindex-all PUT call
                uuids_to_delete = set()
                if not self.skip_comparison:
                    es_uuids = set()
                    index_names = get_all_reindex_enabled_indice_names(self.INDICES)

                    # Get all the uuids that are currently in ES indices
                    for index in index_names.keys():
                        all_indices = index_names[index]
                        # get URL for that index
                        es_url = self.INDICES["indices"][index]["elasticsearch"]["url"].strip("/")

                        for actual_index in all_indices:
                            u = self._get_docs_from_es(
                                index=actual_index,
                                es_url=es_url,
                                fields=["uuid"],
                                session=session,
                            )
                            es_uuids.update(doc["uuid"] for doc in u if "uuid" in doc)

                    # Find uuids that are in ES but not in neo4j, delete them in ES
                    uuids_to_delete = es_uuids - all_entities_uuids
                    if uuids_to_delete:
                        for index in self.index_config:
                            url = index.url.strip("/")
                            with (
                                ESBulkUpdater(
                                    es_url=url, index=index.private, session=session
                                ) as priv_updater,
                                ESBulkUpdater(
                                    es_url=url, index=index.public, session=session
                                ) as pub_updater,
                            ):
                                priv_updater.add_deletes(list(uuids_to_delete))
                                pub_updater.add_deletes(list(uuids_to_delete))

                            delete_failure_results[index.private] = priv_updater.errored_ids
                            delete_failure_results[index.public] = pub_updater.errored_ids

                    # No need to update the entities that were just deleted
                    all_entities_uuids = all_entities_uuids - uuids_to_delete

                failure_results = {}
                for index in self.index_config:
                    failure_results[index.private] = []
                    failure_results[index.public] = []

                all_entities_uuids = list(all_entities_uuids)
                n = self.bulk_update_size
                batched_uuids = [
                    all_entities_uuids[i : i + n] for i in range(0, len(all_entities_uuids), n)
                ]

                if self.memcached_server:
                    client = create_memcached_client(self.memcached_server)
                    progress_writer = MemcachedWriteProgress(
                        client=client,
                        prefix=self.memcached_prefix,
                        num_entites=len(all_entities_uuids),
                    )
                    progress_writer.reset()
                    progress_writer.is_indexing = True

                with ThreadPoolExecutor() as executor:
                    futures = []
                    for index in self.index_config:
                        for uuids in batched_uuids:
                            uuids_copy = deepcopy(uuids)
                            index_copy = deepcopy(index)
                            future = executor.submit(
                                self._upsert_index,
                                entity_uuids=uuids_copy,
                                index=index_copy,
                                progress_writer=progress_writer,
                            )
                            futures.append(future)

                    for f in as_completed(futures):
                        failures = f.result()
                        for index, errored_uuids in failures.items():
                            failure_results[index].extend(errored_uuids)

                end = time.time()

                # Print the results
                nl = "\n"
                update_msg = "\n".join(
                    [
                        f"{index}: {len(uuids)} entities failed to update{nl}{f'{nl}'.join(uuids)}"
                        for index, uuids in failure_results.items()
                    ]
                )
                delete_msg = "\n".join(
                    [
                        f"{index}: {len(uuids)} entities failed to delete{nl}{f'{nl}'.join(uuids)}"
                        for index, uuids in delete_failure_results.items()
                    ]
                )

                msg = (
                    "\n"
                    "============== translate_all() Results ==============\n"
                    f"Total time: {end - start} seconds.\n\n"
                    "Update Results:\n"
                    f"Attempted to update {len(all_entities_uuids)} entities.\n"
                    f"{update_msg}\n\n"
                    "Delete Results:\n"
                    f"Attempted to delete {len(uuids_to_delete) if uuids_to_delete else 0} "
                    f"entities.\n"
                    f"{delete_msg}"
                )
                try:
                    logger.info(msg)
                except OSError:
                    # Too large to print, store in S3
                    s3_url = self._print_to_s3(msg)
                    logger.info(f"Results stored in S3: {s3_url}")

            except Exception as e:
                logger.error(e)
            finally:
                if progress_writer is not None:
                    progress_writer.is_indexing = False
                    progress_writer.percent_complete = 100
                    progress_writer.close()

    def translate(self, entity_id: str):
        start = time.time()
        for index in self.index_config:
            failure_results = self._upsert_index(entity_uuids=[entity_id], index=index)
            nl = "\n"
            update_msg = "\n".join(
                [
                    f"{index}: {len(uuids)} entities failed to update{nl}{f'{nl}'.join(uuids)}"
                    for index, uuids in failure_results.items()
                ]
            )

            msg = (
                "\n"
                "============== translate() Results ==============\n"
                f"Total time: {time.time() - start} seconds.\n\n"
                "Update Results:\n"
                f"Attempted to update 1 entity.\n"
                f"{update_msg}\n"
            )
            try:
                logger.info(msg)
            except OSError:
                # Too large to print, store in S3
                s3_url = self._print_to_s3(msg)
                logger.info(f"Results stored in S3: {s3_url}")

    def update(
        self,
        entity_id: str,
        document: dict,
        index: Optional[str] = None,
        scope: Optional[str] = None,
    ):
        response = ""
        with self._new_session() as session:
            if index is not None and index == "files":
                # The "else clause" is the dominion of the original flavor of OpenSearch indices,
                # for which search-api was created.  This clause is specific to "files" indices, by
                # virtue of the conditions and the following assumption that dataset_uuid is on the
                # JSON body.
                scope_list = self.__get_scope_list(entity_id, document, index, scope, session)

                response = ""
                for scope in scope_list:
                    target_index = self.self_managed_indices[index][scope]
                    if scope == "public" and not self.is_public(document, session=session):
                        # Mimic behavior of "else:" clause for "non-self managed" indices below, and
                        # silently skip public if it was put on the list by __get_scope_list()
                        # because the scope was not explicitly specified.
                        continue
                    response += self.indexer.index(
                        entity_id, json.dumps(document), target_index, True
                    )
                    response += ". "
            else:
                for index in self.indices.keys():
                    public_index = self.INDICES["indices"][index]["public"]
                    private_index = self.INDICES["indices"][index]["private"]

                    if self.is_public(document, session=session):
                        response = self.indexer.index(
                            entity_id,
                            json.dumps(document),
                            public_index,
                            True,
                        )
                    response += self.indexer.index(
                        entity_id, json.dumps(document), private_index, True
                    )

        return response

    def add(
        self,
        entity_id: str,
        document: dict,
        index: Optional[str] = None,
        scope: Optional[str] = None,
    ):
        response = ""
        with self._new_session() as session:
            if index is not None and index == "files":
                # The "else clause" is the dominion of the original flavor of OpenSearch indices,
                # for which search-api was created.  This clause is specific to 'files' indices, by
                # virtue of the conditions and the following assumption that dataset_uuid is on the
                # JSON body.
                scope_list = self.__get_scope_list(entity_id, document, index, scope, session)

                response = ""
                for scope in scope_list:
                    target_index = self.self_managed_indices[index][scope]
                    if scope == "public" and not self.is_public(document, session=session):
                        # Mimic behavior of "else:" clause for "non-self managed" indices below, and
                        # silently skip public if it was put on the list by __get_scope_list()
                        # because the scope was not explicitly specified.
                        continue
                    response += self.indexer.index(
                        entity_id,
                        json.dumps(document),
                        target_index,
                        False,
                    )
                    response += ". "
            else:
                public_index = self.INDICES["indices"][index]["public"]
                private_index = self.INDICES["indices"][index]["private"]

                if self.is_public(document, session=session):
                    response = self.indexer.index(
                        entity_id,
                        json.dumps(document),
                        public_index,
                        False,
                    )
                response += self.indexer.index(
                    entity_id,
                    json.dumps(document),
                    private_index,
                    False,
                )

        return response

    # This method is only applied to Collection/Source/Sample/Dataset/File
    # Collection uses entity-api's logic for "visibility" to determine if a Collection is public or
    # nonpublic
    # For File, if the Dataset of the dataset_uuid element has status=='Published', it may go in a
    #   public index
    # For Dataset, if status=='Published', it goes into the public index
    # For Source/Sample, `data`if any dataset down in the tree is 'Published', they should have
    #   `data_access_level` as public,
    # then they go into public index
    # Don't confuse with `data_access_level`
    def is_public(self, document: dict, session: Optional[Session] = None):
        is_public = False
        close_session = False

        if session is None:
            session = self._new_session()
            close_session = True

        try:
            if "file_uuid" in document:
                # Confirm the Dataset to which the File entity belongs is published
                try:
                    dataset = self._call_entity_api(
                        entity_id=document["dataset_uuid"],
                        endpoint_base="documents",
                        session=session,
                    )
                    return self.is_public(dataset, session=session)
                except Exception:
                    logger.error(
                        f"Failed to confirm if the file's dataset is public for dataset uuid: "
                        f"{document['dataset_uuid']}"
                    )
                    return False

            if document["entity_type"] in ["Dataset", "Publication"]:
                # In case "status" not set
                if "status" in document:
                    if document["status"].lower() == self.DATASET_STATUS_PUBLISHED:
                        is_public = True
                else:
                    # Log as an error to be fixed in Neo4j
                    logger.error(
                        f"{document['entity_type']} of uuid: {document['uuid']} missing 'status' "
                        f"property, treat as not public, verify and set the status."
                    )

            elif document["entity_type"] in ["Collection", "Epicollection"]:
                # If this Collection meets entity-api"s criteria for visibility to the world by
                # returning the value of its schema_constants.py DataVisibilityEnum.PUBLIC,
                # the Collection can be in the public index and retrieved by users who are not
                # logged in.
                try:
                    entity_visibility = self._call_entity_api(
                        entity_id=document["uuid"],
                        endpoint_base="visibility",
                        session=session,
                    )
                    is_public = entity_visibility == "public"
                except Exception:
                    logger.error(
                        f"Failed to confirm if collection is public for uuid: {document['uuid']}"
                    )
                    return False

            elif document["entity_type"] == "Upload":
                is_public = False

            else:
                # In case "data_access_level" not set
                if "data_access_level" in document:
                    if document["data_access_level"].lower() == self.ACCESS_LEVEL_PUBLIC:
                        is_public = True
                else:
                    # Log as an error to be fixed in Neo4j
                    logger.error(
                        f"{document['entity_type']} of uuid: {document['uuid']} missing "
                        f"'data_access_level' property, treat as not public, verify and set the "
                        f"data_access_level."
                    )
        finally:
            if close_session:
                session.close()

        return is_public

    # Private methods

    def _new_session(self):
        session = Session()
        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        session.mount(self.entity_api_url, HTTPAdapter(max_retries=retries))

        for index in self.index_config:
            session.mount(index.url, HTTPAdapter(max_retries=retries))

        return session

    def _call_entity_api(
        self,
        endpoint_base: str,
        session: Session,
        entity_id: Optional[str] = None,
        endpoint_suffix: Optional[str] = None,
        url_property: Optional[str] = None,
        include_token: bool = True,
    ):
        logger.info(
            f"Start executing _call_entity_api() on endpoint_base: {endpoint_base}, "
            f"uuid: {entity_id}"
        )
        url = f"{self.entity_api_url}/{endpoint_base}"
        if entity_id:
            url = f"{url}/{entity_id}"
        if endpoint_suffix:
            url = f"{url}/{endpoint_suffix}"
        if url_property:
            url = f"{url}?property={url_property}"

        headers = self.request_headers if include_token else None
        res = session.get(url, headers=headers, verify=False, timeout=TIMEOUT)

        if res.status_code != 200:
            # Log the full stack trace, prepend a line with our message
            logger.exception(
                f"_call_entity_api() failed on endpoint_base: {endpoint_base}, uuid: {entity_id}"
            )
            logger.debug(
                f"======_call_entity_api() status code from entity-api: {res.status_code}======"
            )
            logger.debug("======_call_entity_api() response text from entity-api======")
            logger.debug(res.text)

            # Add this uuid to the failed list
            self.failed_entity_api_calls.append(url)
            self.failed_entity_ids.append(entity_id)

            # Bubble up the error message from entity-api instead of sys.exit(msg)
            # The caller will need to handle this exception
            res.raise_for_status()
            raise RequestException(res.text)

        logger.info(
            f"Finished executing _call_entity_api() on endpoint_base: {endpoint_base}, "
            f"uuid: {entity_id}"
        )

        # The resulting data can be an entity dict or a list (when `url_property` parameter is
        # specified)
        # For Dataset, data manipulation is performed
        # If result is a list or not a Dataset dict, no change - 7/13/2022 Max & Zhou
        return res.json()

    def _get_docs_from_es(
        self,
        index: str,
        es_url: str,
        fields: list[str],
        session: Session,
        query: Optional[dict] = None,
    ):
        scroll = "1m"  # save scroll context for 1 minute
        size = 10000
        docs = []

        # initial search with scroll and get id
        search_url = f"{es_url}/{index}/_search?scroll={scroll}"
        body = {
            "_source": fields,
            "size": size,
        }
        if query:
            body["query"] = query
        res = session.post(
            search_url,
            json=body,
            timeout=TIMEOUT,
        )
        res.raise_for_status()
        data = res.json()
        scroll_id = data["_scroll_id"]
        hits = data["hits"]["hits"]
        docs.extend(
            {"_id": hit["_id"], **{field: hit["_source"].get(field) for field in fields}}
            for hit in hits
        )

        # scroll until no more hits
        while hits:
            scroll_resp = session.post(
                f"{es_url}/_search/scroll",
                json={"scroll": scroll, "scroll_id": scroll_id},
                timeout=TIMEOUT,
            )
            scroll_resp.raise_for_status()
            scroll_data = scroll_resp.json()
            hits = scroll_data["hits"]["hits"]
            if not hits:
                break
            docs.extend(
                {"_id": hit["_id"], **{field: hit["_source"].get(field) for field in fields}}
                for hit in hits
            )
            scroll_id = scroll_data["_scroll_id"]
            time.sleep(1)

        return docs

    def _upsert_index(
        self,
        entity_uuids: list[str],
        index: Index,
        progress_writer: Optional[MemcachedWriteProgress] = None,
    ):
        failure_results: dict[str, list[str]] = {index.private: [], index.public: []}
        with (
            self._new_session() as session,  # don't share the same session across threads
            ESBulkUpdater(es_url=index.url, index=index.private, session=session) as priv_updater,
            ESBulkUpdater(es_url=index.url, index=index.public, session=session) as pub_updater,
        ):
            # get doc_sha256s
            private_sha256s = {
                doc["uuid"]: doc["doc_sha256"]
                for doc in self._get_docs_from_es(
                    index=index.private,
                    es_url=index.url,
                    fields=["uuid", "doc_sha256"],
                    session=session,
                    query={"terms": {"uuid.keyword": entity_uuids}},
                )
                if "uuid" in doc and "doc_sha256" in doc
            }

            publish_sha256s = {
                doc["uuid"]: doc["doc_sha256"]
                for doc in self._get_docs_from_es(
                    index=index.public,
                    es_url=index.url,
                    fields=["uuid", "doc_sha256"],
                    session=session,
                    query={"terms": {"uuid.keyword": entity_uuids}},
                )
                if "uuid" in doc and "doc_sha256" in doc
            }

            failure_results = {index.private: [], index.public: []}
            for entity_uuid in entity_uuids:
                try:
                    # Retrieve the private document
                    priv_entity = self._call_entity_api(
                        endpoint_base="documents",
                        entity_id=entity_uuid,
                        include_token=True,
                        session=session,
                    )
                    actual_sha256 = self._calculate_doc_sha256_hash(priv_entity)

                    if private_sha256s.get(entity_uuid) is None:
                        logger.info(f"Entity {entity_uuid} is new, adding to private ES")
                        # entity does not exist in ES,
                        priv_entity["doc_sha256"] = actual_sha256
                        priv_updater.add_update(doc_id=entity_uuid, doc=priv_entity, upsert=True)

                    elif private_sha256s.get(entity_uuid) != actual_sha256:
                        logger.info(f"Entity {entity_uuid} has changed, updating in private ES")
                        # entity exists but has changed, update it in ES
                        priv_entity["doc_sha256"] = actual_sha256
                        priv_updater.add_delete(doc_id=entity_uuid)
                        priv_updater.add_update(doc_id=entity_uuid, doc=priv_entity, upsert=True)

                    elif private_sha256s.get(entity_uuid) == actual_sha256:
                        logger.info(
                            f"Entity {entity_uuid} has not changed, skipping update in private ES"
                        )
                        pass

                except Exception as e:
                    failure_results[index.private].append(f"{entity_uuid}: Update - {str(e)}")
                    logger.exception(e)
                    continue

                if self.is_public(priv_entity, session=session):
                    try:
                        # Retrieve the public document
                        pub_entity = self._call_entity_api(
                            endpoint_base="documents",
                            entity_id=entity_uuid,
                            include_token=False,
                            session=session,
                        )
                        actual_sha256 = self._calculate_doc_sha256_hash(pub_entity)

                        if publish_sha256s.get(entity_uuid) is None:
                            # entity does not exist in ES, insert it into ES
                            logger.info(f"Entity {entity_uuid} is new, adding to public ES")
                            pub_entity["doc_sha256"] = actual_sha256
                            pub_updater.add_update(doc_id=entity_uuid, doc=pub_entity, upsert=True)

                        elif publish_sha256s.get(entity_uuid) != actual_sha256:
                            # entity exists but has changed, delete and insert it in ES
                            logger.info(f"Entity {entity_uuid} has changed, updating in public ES")
                            pub_entity["doc_sha256"] = actual_sha256
                            pub_updater.add_delete(doc_id=entity_uuid)
                            pub_updater.add_update(doc_id=entity_uuid, doc=pub_entity, upsert=True)

                        elif publish_sha256s.get(entity_uuid) == actual_sha256:
                            logger.info(
                                f"Entity {entity_uuid} has not changed, skipping update in "
                                f"public ES"
                            )
                            pass

                    except Exception as e:
                        failure_results[index.public].append(f"{entity_uuid}: Update - {str(e)}")
                        logger.exception(e)
                        pass

        if progress_writer is not None:
            progress_writer.add_entities_complete(len(entity_uuids))

        if priv_updater.errored_ids:
            failure_results[index.private].extend(priv_updater.errored_ids)
        if pub_updater.errored_ids:
            failure_results[index.public].extend(pub_updater.errored_ids)

        return failure_results

    def _calculate_doc_sha256_hash(self, doc: Any) -> str:
        norm_obj = self._normalized_obj(doc)
        norm_json = json.dumps(norm_obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(norm_json.encode("utf-8")).hexdigest()

    def _normalized_obj(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            return {str(k): self._normalized_obj(v) for k, v in obj.items()}

        if isinstance(obj, (list, tuple)):
            items = [self._normalized_obj(v) for v in obj]
            try:
                items.sort(
                    key=lambda x: json.dumps(
                        x, sort_keys=True, separators=(",", ":"), ensure_ascii=False
                    )
                )
            except Exception:
                # fallback to original order if something goes wrong
                pass
            return items

        if isinstance(obj, set):
            items = [self._normalized_obj(v) for v in obj]
            items.sort(
                key=lambda x: json.dumps(
                    x, sort_keys=True, separators=(",", ":"), ensure_ascii=False
                )
            )
            return items

        if isinstance(obj, bytes):
            return obj.hex()

        if isinstance(obj, decimal.Decimal):
            return str(obj)

        if isinstance(obj, datetime.datetime):
            return obj.isoformat()

        if isinstance(obj, datetime.date):
            return obj.isoformat()

        if obj is None or isinstance(obj, (str, int, float, bool)):
            if isinstance(obj, float):
                if math.isnan(obj) or math.isinf(obj):
                    return str(obj)

            return obj

        if hasattr(obj, "__dict__"):
            return self._normalized_obj(obj.__dict__)

        return str(obj)

    def _print_to_s3(self, text: str):
        # We must load from config file since we're not in the flask app context
        file_dir = os.path.abspath(os.path.dirname(__file__))
        cfg_dir = os.path.join(file_dir, "instance")
        cfg = Config(cfg_dir)
        if cfg.from_pyfile("app.cfg") is False:
            raise ValueError("Failed to load app.cfg while trying to print to S3")

        s3_worker = S3Worker(
            cfg["AWS_ACCESS_KEY_ID"],
            cfg["AWS_SECRET_ACCESS_KEY"],
            cfg["AWS_S3_BUCKET_NAME"],
            cfg["AWS_OBJECT_URL_EXPIRATION_IN_SECS"],
            0,  # always meets the threshold
            cfg["AWS_S3_OBJECT_RESULTS_PREFIX"],
        )
        url = s3_worker.stash_response_body_if_big(text)
        return url

    def _bulk_update_es(self, bulk_update: BulkUpdate, index: str, es_url: str, session: Session):
        if not bulk_update.upserts and not bulk_update.deletes:
            raise ValueError("No upserts or deletes provided.")

        url = f"{es_url}/{index}/_bulk"
        headers = {"Content-Type": "application/x-ndjson"}

        # Preparing ndjson content
        upserts = [
            f'{{"update":{{"_id":"{upsert["uuid"]}"}}}}\n{{"doc":{json.dumps(upsert, separators=(",", ":"))},"doc_as_upsert":true}}'
            for upsert in bulk_update.upserts
        ]
        deletes = [
            f'{{ "delete": {{ "_id": "{delete_uuid}" }} }}' for delete_uuid in bulk_update.deletes
        ]

        body = "\n".join(deletes + upserts) + "\n"

        return session.post(url, headers=headers, data=body, timeout=TIMEOUT)

    def __get_scope_list(
        self,
        entity_id: str,
        document: dict,
        index: str,
        scope: Optional[str],
        session: Session,
    ):
        scope_list = []
        if index == "files":
            # It would be nice if the possible scopes could be identified from
            # self.INDICES['indices'] rather than hardcoded. @TODO
            # This can handle indices besides "files" which might accept "scope" as
            # an argument, but returning an empty list, not raising an Exception, for
            # an  unrecognized index name.
            if scope is not None:
                if scope not in ["public", "private"]:
                    msg = (
                        f"Unrecognized scope '{scope}' requested for"
                        f" entity_id '{entity_id}' in Dataset '{document['dataset_uuid']}."
                    )
                    logger.info(msg)
                    raise ValueError(msg)
                elif scope == "public":
                    if self.is_public(document, session=session):
                        scope_list.append(scope)
                    else:
                        # Reject the addition of 'public' was explicitly indicated, even though
                        # the public index may be silently skipped when a scope is not specified, in
                        # order to mimic behavior below for "non-self managed" indices.
                        msg = (
                            f"Dataset '{document['dataset_uuid']}"
                            f" does not have status {self.DATASET_STATUS_PUBLISHED}, so"
                            f" entity_id '{entity_id}' cannot go in a public index."
                        )
                        logger.info(msg)
                        raise ValueError(msg)
                elif scope == "private":
                    scope_list.append(scope)
            else:
                scope_list = ["public", "private"]

        return scope_list


# This approach is different from the live reindex via HTTP request
# It'll delete all the existing indices and recreate then then index everything
if __name__ == "__main__":
    # Specify the absolute path of the instance folder and use the config file relative to the
    # instance path
    app = Flask(
        __name__,
        instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__)), "../src/instance"),
        instance_relative_config=True,
    )
    app.config.from_pyfile("app.cfg")
    ubkg_instance = initialize_ubkg(app.config)

    INDICES = safe_load(
        (Path(__file__).absolute().parent / "instance/search-config.yaml").read_text()
    )
    app.config["INDICES"] = INDICES
    app.config["DEFAULT_INDEX_WITHOUT_PREFIX"] = INDICES["default_index"]
    app.config["DEFAULT_ELASTICSEARCH_URL"] = INDICES["indices"][
        app.config["DEFAULT_INDEX_WITHOUT_PREFIX"]
    ]["elasticsearch"]["url"].strip("/")
    app.config["DEFAULT_ENTITY_API_URL"] = INDICES["indices"][
        app.config["DEFAULT_INDEX_WITHOUT_PREFIX"]
    ]["document_source_endpoint"].strip("/")

    try:
        token = sys.argv[1]
    except IndexError:
        msg = "Missing admin group token argument"
        logger.exception(msg)
        sys.exit(msg)

    # Create an instance of the indexer
    translator = Translator(config=app.config, ubkg_instance=ubkg_instance, token=token)

    auth_helper = translator.init_auth_helper()

    # The second argument indicates to get the groups information
    user_info_dict = auth_helper.getUserInfo(token, True)

    if isinstance(user_info_dict, Response):
        msg = "The given token is expired or invalid"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        sys.exit(msg)

    # Use the new key rather than the 'hmgroupids' which will be deprecated
    group_ids = user_info_dict["group_membership_ids"]

    # Ensure the user belongs to the Globus Data Admin group
    if not auth_helper.has_data_admin_privs(token):
        msg = "The given token doesn't belong to the Globus Data Admin group, access not granted"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        sys.exit(msg)

    start = time.time()
    logger.info("############# Full index via script started #############")

    translator.delete_and_recreate_indices(specific_index=None)
    translator.delete_and_recreate_indices(specific_index="files")
    # translator.translate_all()

    # Show the failed entity-api calls and the uuids
    if translator.failed_entity_api_calls:
        logger.info(f"{len(translator.failed_entity_api_calls)} entity-api calls failed")
        print(*translator.failed_entity_api_calls, sep="\n")

    if translator.failed_entity_ids:
        logger.info(f"{len(translator.failed_entity_ids)} entity ids failed")
        print(*translator.failed_entity_ids, sep="\n")

    end = time.time()
    logger.info(
        f"############# Full index via script completed. Total time used: {end - start} "
        f"seconds. #############"
    )
