import concurrent.futures
import copy
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests
from atlas_consortia_commons.object import enum_val
from atlas_consortia_commons.ubkg import initialize_ubkg
from flask import Config, Flask, Response
from hubmap_commons.hm_auth import AuthHelper  # HuBMAP commons
from hubmap_commons.S3_worker import S3Worker
from requests.adapters import HTTPAdapter, Retry
from yaml import safe_load

if "search-adaptor/src" not in sys.path:
    sys.path.append("search-adaptor/src")

from indexer import Indexer
from opensearch_helper_functions import BulkUpdate, bulk_update, get_uuids_from_es
from translator.tranlation_helper_functions import get_all_reindex_enabled_indice_names
from translator.translator_interface import TranslatorInterface

from libs.memcached_progress import MemcachedWriteProgress, create_memcached_client
from libs.ontology import Ontology

logging.basicConfig(
    format="[%(asctime)s] %(levelname)s in %(module)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


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
    indexer = None
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

        except Exception:
            raise ValueError("Invalid indices config")

        self.app_client_id = config["APP_CLIENT_ID"]
        self.app_client_secret = config["APP_CLIENT_SECRET"]
        self.memcached_server = config["MEMCACHED_SERVER"] if config["MEMCACHED_MODE"] else None
        self.memcached_prefix = config["MEMCACHED_PREFIX"]
        self.token = token

        self.request_headers = self._create_request_headers_for_auth(token)
        self.entity_api_url = self.indices[self.DEFAULT_INDEX_WITHOUT_PREFIX][
            "document_source_endpoint"
        ].strip("/")

        # Add index_version by parsing the VERSION file
        self.index_version = (
            (Path(__file__).absolute().parent.parent / "VERSION").read_text()
        ).strip()

        self.bulk_update_size = indices.get("bulk_update_size", 50)

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

                self._delete_index(public_index)
                self._delete_index(private_index)

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

                    self._delete_index(public_index)
                    self._delete_index(private_index)

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
                    session=session,
                    endpoint_base="source",
                    endpoint_suffix="entities",
                    url_property="uuid",
                )
                upload_uuids_list = self._call_entity_api(
                    session=session,
                    endpoint_base="upload",
                    endpoint_suffix="entities",
                    url_property="uuid",
                )
                collection_uuids_list = self._call_entity_api(
                    session=session,
                    endpoint_base="collection",
                    endpoint_suffix="entities",
                    url_property="uuid",
                )
                sample_uuids_list = self._call_entity_api(
                    session=session,
                    endpoint_base="sample",
                    endpoint_suffix="entities",
                    url_property="uuid",
                )
                dataset_uuids_list = self._call_entity_api(
                    session=session,
                    endpoint_base="dataset",
                    endpoint_suffix="entities",
                    url_property="uuid",
                )

                # Merge into a big list that with no duplicates
                all_entities_uuids = set(
                    source_uuids_list
                    + sample_uuids_list
                    + dataset_uuids_list
                    + upload_uuids_list
                    + collection_uuids_list
                )

                # Only need this comparison for the live /reindex-all PUT call
                if not self.skip_comparison:
                    es_uuids = set()
                    index_names = get_all_reindex_enabled_indice_names(self.INDICES)

                    for index in index_names.keys():
                        all_indices = index_names[index]
                        # get URL for that index
                        es_url = self.INDICES["indices"][index]["elasticsearch"]["url"].strip("/")

                        for actual_index in all_indices:
                            es_uuids.update(get_uuids_from_es(actual_index, es_url))

                    # Find uuids that are in ES but not in neo4j, delete them in ES
                    uuids_to_delete = es_uuids - all_entities_uuids
                    update = BulkUpdate(deletes=list(uuids_to_delete))
                    for index in self.index_config:
                        failures = self._bulk_update(update, index.private, index.url, session)
                        delete_failure_results[index.private] = failures
                        failures = self._bulk_update(update, index.public, index.url, session)
                        delete_failure_results[index.public] = failures

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

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = []
                    for index in self.index_config:
                        for uuids in batched_uuids:
                            uuids_copy = copy.deepcopy(uuids)
                            index_copy = copy.deepcopy(index)
                            future = executor.submit(
                                self._upsert_index,
                                entity_ids=uuids_copy,
                                index=index_copy,
                                session=session,
                                priv_entities=[],
                                pub_entities=[],
                                progress_writer=progress_writer,
                            )
                            futures.append(future)

                    for f in concurrent.futures.as_completed(futures):
                        failures = f.result()
                        for index, uuids in failures.items():
                            failure_results[index].extend(uuids)

                end = time.time()

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

                try:
                    msg = (
                        "\n"
                        "============== translate_all() Results ==============\n"
                        f"Total time: {end - start} seconds.\n\n"
                        "Update Results:\n"
                        f"Attempted to update {len(all_entities_uuids)} entities.\n"
                        f"{update_msg}\n\n"
                        "Delete Results:\n"
                        f"Attempted to delete {len(uuids_to_delete) if uuids_to_delete else 0} entities.\n"
                        f"{delete_msg}"
                    )
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
        with self._new_session() as session:
            try:
                priv_document = self._call_entity_api(
                    session=session,
                    entity_id=entity_id,
                    endpoint_base="documents",
                    include_token=True,
                )
                logger.info(
                    f"Start executing translate() on {priv_document['entity_type']} of uuid: {priv_document['uuid']}"
                )

                pub_document = None
                if self.is_public(priv_document):
                    try:
                        pub_document = self._call_entity_api(
                            session=session,
                            entity_id=entity_id,
                            endpoint_base="documents",
                            include_token=False,
                        )
                    except Exception:
                        pass

                failure_results = {}
                for index in self.index_config:
                    results = self._upsert_index(
                        entity_ids=[],
                        index=index,
                        session=session,
                        priv_entities=[priv_document],
                        pub_entities=[pub_document] if pub_document else [],
                        progress_writer=None,
                    )
                    failure_results.update(results)

                end = time.time()

                nl = "\n"
                msg = "\n".join(
                    [
                        f"{index}: {len(uuids)} entities failed{nl}{f'{nl}'.join(uuids)}"
                        for index, uuids in failure_results.items()
                    ]
                )
                logger.info(
                    "\n"
                    "============== translate() Results ==============\n"
                    f"uuid: {entity_id}, entity_type: {priv_document['entity_type']}\n"
                    f"Total time used: {end - start} seconds.\n"
                    "Results:\n"
                    f"Attempted to update 1 entity.\n"
                    f"{msg}"
                )
            except Exception as e:
                msg = f"Exceptions during executing translate(): {e}"
                # Log the full stack trace, prepend a line with our message
                logger.exception(msg)

    def update(self, entity_id, document, index=None, scope=None):
        if index is not None and index == "files":
            # The "else clause" is the dominion of the original flavor of OpenSearch indices, for which search-api
            # was created.  This clause is specific to "files" indices, by virtue of the conditions and the
            # following assumption that dataset_uuid is on the JSON body.
            scope_list = self.__get_scope_list(entity_id, document, index, scope)

            response = ""
            for scope in scope_list:
                target_index = self.self_managed_indices[index][scope]
                if scope == "public" and not self.is_public(document):
                    # Mimic behavior of "else:" clause for "non-self managed" indices below, and
                    # silently skip public if it was put on the list by __get_scope_list() because
                    # the scope was not explicitly specified.
                    continue
                response += self.indexer.index(entity_id, json.dumps(document), target_index, True)
                response += ". "
        else:
            for index in self.indices.keys():
                public_index = self.INDICES["indices"][index]["public"]
                private_index = self.INDICES["indices"][index]["private"]

                if self.is_public(document):
                    response = self.indexer.index(
                        entity_id, json.dumps(document), public_index, True
                    )
                response += self.indexer.index(entity_id, json.dumps(document), private_index, True)

        return response

    def add(self, entity_id, document, index=None, scope=None):
        if index is not None and index == "files":
            # The "else clause" is the dominion of the original flavor of OpenSearch indices, for which search-api
            # was created.  This clause is specific to 'files' indices, by virtue of the conditions and the
            # following assumption that dataset_uuid is on the JSON body.
            scope_list = self.__get_scope_list(entity_id, document, index, scope)

            response = ""
            for scope in scope_list:
                target_index = self.self_managed_indices[index][scope]
                if scope == "public" and not self.is_public(document):
                    # Mimic behavior of "else:" clause for "non-self managed" indices below, and
                    # silently skip public if it was put on the list by __get_scope_list() because
                    # the scope was not explicitly specified.
                    continue
                response += self.indexer.index(entity_id, json.dumps(document), target_index, False)
                response += ". "
        else:
            public_index = self.INDICES["indices"][index]["public"]
            private_index = self.INDICES["indices"][index]["private"]

            if self.is_public(document):
                response = self.indexer.index(entity_id, json.dumps(document), public_index, False)
            response += self.indexer.index(entity_id, json.dumps(document), private_index, False)

        return response

    # This method is only applied to Collection/Source/Sample/Dataset/File
    # Collection uses entity-api's logic for "visibility" to determine if a Collection is public or nonpublic
    # For File, if the Dataset of the dataset_uuid element has status=='Published', it may go in a public index
    # For Dataset, if status=='Published', it goes into the public index
    # For Source/Sample, `data`if any dataset down in the tree is 'Published', they should have `data_access_level` as public,
    # then they go into public index
    # Don't confuse with `data_access_level`
    def is_public(self, document: dict):
        is_public = False

        if "file_uuid" in document:
            # Confirm the Dataset to which the File entity belongs is published
            try:
                dataset = self._call_entity_api(
                    entity_id=document["dataset_uuid"], endpoint_base="documents"
                )
                return self.is_public(dataset)
            except Exception:
                logger.error(
                    f"Failed to confirm if the file's dataset is public for dataset uuid: {document['dataset_uuid']}"
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
                    f"{document['entity_type']} of uuid: {document['uuid']} missing 'status' property, "
                    "treat as not public, verify and set the status."
                )

        elif document["entity_type"] in ["Collection"]:
            # If this Collection meets entity-api"s criteria for visibility to the world by
            # returning the value of its schema_constants.py DataVisibilityEnum.PUBLIC,
            # the Collection can be in the public index and retrieved by users who are not logged in.
            try:
                entity_visibility = self._call_entity_api(
                    entity_id=document["uuid"], endpoint_base="visibility"
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
                    f"{document['entity_type']} of uuid: {document['uuid']} missing 'data_access_level' "
                    "property, treat as not public, verify and set the data_access_level."
                )

        return is_public

    # Private methods

    # Create a dict with HTTP Authorization header with Bearer token
    def _create_request_headers_for_auth(self, token: str):
        auth_header_name = "Authorization"
        auth_scheme = "Bearer"

        headers_dict = {
            # Don't forget the space between scheme and the token value
            auth_header_name: auth_scheme
            + " "
            + token
        }

        return headers_dict

    def _upsert_index(
        self,
        entity_ids: list[str],
        index: Index,
        session: requests.Session,
        priv_entities: list[dict],
        pub_entities: list[dict],
        progress_writer: Optional[MemcachedWriteProgress] = None,
    ):
        failure_results = {index.private: [], index.public: []}
        for entity_id in entity_ids:
            try:
                # Retrieve the private document
                priv_entity = self._call_entity_api(
                    session=session,
                    entity_id=entity_id,
                    endpoint_base="documents",
                    include_token=True,
                )
                priv_entities.append(priv_entity)
            except Exception as e:
                failure_results[index.private].append(f"{entity_id}: Update - {str(e)}")
                logger.exception(e)
                continue

            if self.is_public(priv_entity):
                try:
                    # Retrieve the public document
                    pub_entity = self._call_entity_api(
                        session=session,
                        entity_id=entity_id,
                        endpoint_base="documents",
                        include_token=False,
                    )
                    pub_entities.append(pub_entity)
                except Exception as e:
                    failure_results[index.public].append(f"{entity_id}: Update - {str(e)}")
                    pass

            # Send bulk update when the batch size is reached
            max_size = self.bulk_update_size
            if len(priv_entities) >= max_size or len(pub_entities) >= max_size:
                priv_update = BulkUpdate(upserts=priv_entities)
                pub_update = BulkUpdate(upserts=pub_entities)

                failures = self._bulk_update(priv_update, index.private, index.url, session)
                failure_results[index.private].extend(failures)

                failures = self._bulk_update(pub_update, index.public, index.url, session)
                failure_results[index.public].extend(failures)

                if progress_writer:
                    incr = max(len(priv_entities), len(pub_entities))
                    progress_writer.add_entities_complete(incr)

                priv_entities = []
                pub_entities = []

        # Send bulk update for the remaining entities
        if priv_entities:
            priv_update = BulkUpdate(upserts=priv_entities)
            failures = self._bulk_update(priv_update, index.private, index.url, session)
            failure_results[index.private].extend(failures)
        if pub_entities:
            pub_update = BulkUpdate(upserts=pub_entities)
            failures = self._bulk_update(pub_update, index.public, index.url, session)
            failure_results[index.public].extend(failures)
        if progress_writer:
            incr = max(len(priv_entities), len(pub_entities))
            progress_writer.add_entities_complete(incr)

        return failure_results

    def _bulk_update(
        self, updates: BulkUpdate, index: str, es_url: str, session: requests.Session = None
    ):
        if not updates.upserts and not updates.deletes:
            return []

        res = bulk_update(bulk_update=updates, index=index, es_url=es_url, session=session)
        if res.status_code == 413:
            # If the request is too large, split the request in half and try again, recursively
            upsert_half = len(updates.upserts) // 2
            delete_half = len(updates.deletes) // 2
            first_half = BulkUpdate(
                upserts=updates.upserts[:upsert_half], deletes=updates.deletes[:delete_half]
            )
            second_half = BulkUpdate(
                upserts=updates.upserts[upsert_half:], deletes=updates.deletes[delete_half:]
            )
            return self._bulk_update(first_half, index, es_url, session) + self._bulk_update(
                second_half, index, es_url, session
            )

        if res.status_code != 200:
            logger.error(f"Failed to bulk update index: {index} in elasticsearch.")
            logger.error(f"Error Message: {res.text}")
            failure_uuids = [f"{upsert['uuid']}: Update - {res.text}" for upsert in updates.upserts]
            failure_uuids.extend(
                [f"{delete_uuid}: Delete - {res.text}" for delete_uuid in updates.deletes]
            )
            return failure_uuids

        res_body = res.json().get("items", [])
        result_values = [
            item.get("update") or item.get("delete")
            for item in res_body
            if "update" in item or "delete" in item
        ]

        failure_uuids = [
            f"{item['_id']}: Update - {item.get('error', {}).get('reason')}"
            for item in result_values
            if item["status"] not in [200, 201]
        ]
        return failure_uuids

    # This method is supposed to only retrieve Dataset|Source|Sample
    # The Collection and Upload are handled by separate calls
    # The returned data can either be an entity dict or a list of uuids (when `url_property` parameter is specified)
    def _call_entity_api(
        self,
        endpoint_base: str,
        entity_id: Optional[str] = None,
        endpoint_suffix: Optional[str] = None,
        url_property: Optional[str] = None,
        include_token: bool = True,
        session: Optional[requests.Session] = None,
    ):

        logger.info(
            f"Start executing _call_entity_api() on endpoint_base: {endpoint_base}, uuid: {entity_id}"
        )
        url = f"{self.entity_api_url}/{endpoint_base}"
        if entity_id:
            url = f"{url}/{entity_id}"
        if endpoint_suffix:
            url = f"{url}/{endpoint_suffix}"
        if url_property:
            url = f"{url}?property={url_property}"

        headers = self.request_headers if include_token else None
        if session:
            response = session.get(url, headers=headers, verify=False, timeout=10)
        else:
            response = requests.get(url, headers=headers, verify=False, timeout=10)

        if response.status_code != 200:
            # Log the full stack trace, prepend a line with our message
            logger.exception(
                f"_call_entity_api() failed on endpoint_base: {endpoint_base}, uuid: {entity_id}"
            )
            logger.debug(
                f"======_call_entity_api() status code from entity-api: {response.status_code}======"
            )
            logger.debug("======_call_entity_api() response text from entity-api======")
            logger.debug(response.text)

            # Add this uuid to the failed list
            self.failed_entity_api_calls.append(url)
            self.failed_entity_ids.append(entity_id)

            # Bubble up the error message from entity-api instead of sys.exit(msg)
            # The caller will need to handle this exception
            response.raise_for_status()
            raise requests.exceptions.RequestException(response.text)

        logger.info(
            f"Finished executing _call_entity_api() on endpoint_base: {endpoint_base}, uuid: {entity_id}"
        )

        # The resulting data can be an entity dict or a list (when `url_property` parameter is specified)
        # For Dataset, data manipulation is performed
        # If result is a list or not a Dataset dict, no change - 7/13/2022 Max & Zhou
        return response.json()

    def _delete_index(self, index: str):
        try:
            self.indexer.delete_index(index)
        except Exception:
            pass

    def _new_session(self):
        session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        session.mount(self.entity_api_url, HTTPAdapter(max_retries=retries))

        for index in self.index_config:
            session.mount(index.url, HTTPAdapter(max_retries=retries))

        return session

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


def get_val_by_key(type_code, data, source_data_name):
    # Use triple {{{}}}
    result_val = f"{{{type_code}}}"

    if type_code in data:
        result_val = data[type_code]
    else:
        # Return the error message as result
        logger.error(f"Missing key {type_code} in {source_data_name}")

    logger.debug(f"======== get_val_by_key: {result_val}")

    return result_val


# This approach is different from the live reindex via HTTP request
# It'll delete all the existing indices and recreate then then index everything
if __name__ == "__main__":
    # Specify the absolute path of the instance folder and use the config file relative to the instance path
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
        f"############# Full index via script completed. Total time used: {end - start} seconds. #############"
    )
