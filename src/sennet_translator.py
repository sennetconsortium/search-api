import concurrent.futures
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import requests
from atlas_consortia_commons.object import enum_val
from atlas_consortia_commons.string import equals
from atlas_consortia_commons.ubkg import initialize_ubkg
from flask import Flask, Response
from hubmap_commons.hm_auth import AuthHelper  # HuBMAP commons
from yaml import safe_load

from libs.ontology import Ontology

sys.path.append("search-adaptor/src")

from indexer import Indexer
from opensearch_helper_functions import execute_opensearch_query, get_uuids_from_es
from translator.tranlation_helper_functions import get_all_reindex_enabled_indice_names, get_uuids_by_entity_type
from translator.translator_interface import TranslatorInterface

logging.basicConfig(
    format="[%(asctime)s] %(levelname)s in %(module)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

entity_properties_list = [
    "metadata",
    "source",
    "origin_sample",
    "source_sample",
    "ancestor_ids",
    "descendant_ids",
    "ancestors",
    "descendants",
    "files",
    "immediate_ancestors",
    "immediate_descendants",
    "datasets",
    "entities",
]

# A map keyed by entity attribute names stored in Neo4j and retrieved from entity-api, with
# values for the corresponding document field name in OpenSearch. This Python dict only includes
# attribute names which must be transformed, unlike the neo4j-to-es-attributes.json is replaces.
neo4j_to_es_attribute_name_map = {"ingest_metadata": "metadata"}

# Entity types that will have `display_subtype` generated ar index time
entity_types_with_display_subtype = [
    "Upload",
    "Source",
    "Sample",
    "Dataset",
    "Publication",
]


@dataclass(frozen=True)
class BulkUpdate:
    upserts: list[dict] = field(default_factory=list)
    deletes: list[str] = field(default_factory=list)


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
    BULK_UPDATE_SIZE = 50

    failed_entity_api_calls = []
    failed_entity_ids = []
    indexer = None
    skip_comparison = False
    transformation_resources = {}  # Not used in SenNet

    def __init__(self, indices, app_client_id, app_client_secret, token, ubkg_instance=None):
        try:
            self.ingest_api_soft_assay_url = indices["ingest_api_soft_assay_url"].strip("/")
            self.indices: dict = {}
            self.self_managed_indices: dict = {}
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
            self.DEFAULT_ENTITY_API_URL = self.INDICES["indices"][self.DEFAULT_INDEX_WITHOUT_PREFIX]["document_source_endpoint"].strip("/")

            self.indexer = Indexer(self.indices, self.DEFAULT_INDEX_WITHOUT_PREFIX)
            self.ubkg_instance = ubkg_instance
            Ontology.set_instance(self.ubkg_instance)

            self.entity_types = Ontology.ops(as_arr=True, cb=enum_val).entities()
            self.entities = Ontology.ops().entities()

            self.index_config = [
                Index(name=index, url=cfg["elasticsearch"]["url"], public=cfg["public"], private=cfg["private"])
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

        self.app_client_id = app_client_id
        self.app_client_secret = app_client_secret
        self.token = token

        self.request_headers = self._create_request_headers_for_auth(token)
        self.entity_api_url = self.indices[self.DEFAULT_INDEX_WITHOUT_PREFIX]["document_source_endpoint"].strip("/")

        # Add index_version by parsing the VERSION file
        self.index_version = ((Path(__file__).absolute().parent.parent / "VERSION").read_text()).strip()

    # Public methods

    def init_auth_helper(self):
        if AuthHelper.isInitialized() is False:
            auth_helper = AuthHelper.create(self.app_client_id, self.app_client_secret)
        else:
            auth_helper = AuthHelper.instance()

        return auth_helper

    def delete_and_recreate_indices(self, files: bool = False):
        try:
            logger.info("Start executing delete_and_recreate_indices()")
            public_index = None
            private_index = None

            # Delete and recreate target indices
            if files:
                for index in self.self_managed_indices.keys():
                    public_index = self.self_managed_indices[index]["public"]
                    private_index = self.self_managed_indices[index]["private"]

                    self._delete_index(public_index)
                    self._delete_index(private_index)

                    index_mapping_file = self.self_managed_indices[index]["elasticsearch"]["mappings"]

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
        try:
            logger.info("############# Reindex Live Started #############")
            start = time.time()
            delete_failure_results = {}

            source_uuids_list = self._call_entity_api(endpoint_base="source", endpoint_suffix="entities", url_property="uuid")
            upload_uuids_list = self._call_entity_api(endpoint_base="upload", endpoint_suffix="entities",  url_property="uuid")
            collection_uuids_list = self._call_entity_api(endpoint_base="collection", endpoint_suffix="entities", url_property="uuid")

            # Only need this comparison for the live /reindex-all PUT call
            if not self.skip_comparison:
                # Make calls to entity-api to get a list of uuids for rest of entity types
                sample_uuids_list = self._call_entity_api(endpoint_base="sample", endpoint_suffix="entities", url_property="uuid")
                dataset_uuids_list = self._call_entity_api(endpoint_base="dataset", endpoint_suffix="entities", url_property="uuid")

                # Merge into a big list that with no duplicates
                all_entities_uuids = set(
                    source_uuids_list
                    + sample_uuids_list
                    + dataset_uuids_list
                    + upload_uuids_list
                    + collection_uuids_list
                )

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
                    failures = self._bulk_update(update, index.private, index.url)
                    delete_failure_results[index.private] = failures
                    failures = self._bulk_update(update, index.public, index.url)
                    delete_failure_results[index.public] = failures

            failure_results = {}
            all_entities_uuids = list(all_entities_uuids)
            n = self.BULK_UPDATE_SIZE
            batched_uuids = [all_entities_uuids[i:i + n] for i in range(0, len(all_entities_uuids), n)]
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                for index in self.index_config:
                    for uuids in batched_uuids:
                        futures.extend([executor.submit(self._upsert_index, uuids, index) for uuids in batched_uuids])

                for f in concurrent.futures.as_completed(futures):
                    failures = f.result()
                    for index, uuids in failures.items():
                        failure_results.get(index, []).extend(uuids)

            end = time.time()

            update_msg = "\n".join([
                f"{index}: {len(uuids)} entities failed to update {', '.join(uuids)}"
                for index, uuids in failure_results.items()
            ])
            delete_msg = "\n".join([
                f"{index}: {len(uuids)} entities failed to delete {', '.join(uuids)}"
                for index, uuids in delete_failure_results.items()
            ])

            logger.info(
                "\n"
                "============== translate() Results ==============\n"
                f"Finished executing translateAll().\n"
                f"Total time: {end - start} seconds.\n"
                "Update Results:\n"
                f"{update_msg}\n"
                "Delete Results:\n"
                f"{delete_msg}"
            )

        except Exception as e:
            logger.error(e)

    def translate(self, entity_id: str):
        start = time.time()
        try:
            priv_document = self._call_entity_api(entity_id=entity_id, endpoint_base="documents", include_token=True)
            logger.info(f"Start executing translate() on {priv_document['entity_type']} of uuid: {priv_document['uuid']}")

            pub_document = None
            if self.is_public(priv_document):
                try:
                    pub_document = self._call_entity_api(entity_id=entity_id, endpoint_base="documents", include_token=False)
                except Exception:
                    pass

            entities_to_update = set()
            if equals(priv_document["entity_type"], self.entities.DATASET):
                # Get any Collections and Uploads which reference this Dataset entity
                entities_to_update.update(self._call_entity_api(
                    entity_id=entity_id,
                    endpoint_base="entities",
                    endpoint_suffix="collections",
                    url_property="uuid",
                ))
                entities_to_update.update(self._call_entity_api(
                    entity_id=entity_id,
                    endpoint_base="entities",
                    endpoint_suffix="uploads",
                    url_property="uuid",
                ))

            failure_results = {}
            for index in self.index_config:
                results = self._upsert_index(
                    entity_ids=entities_to_update,
                    index=index,
                    priv_entities=[priv_document],
                    pub_entities=[pub_document] if pub_document else []
                )
                failure_results.update(results)

            end = time.time()
            msg = "\n".join([
                f"{index}: {len(uuids)} entities failed {', '.join(uuids)}"
                for index, uuids in failure_results.items()
            ])
            logger.info(
                "\n"
                "============== translate() Results ==============\n"
                f"Finished executing translate() on {priv_document['entity_type']} of uuid: {entity_id}.\n"
                f"Total time used: {end - start} seconds.\n"
                "Results:\n"
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
                    response = self.indexer.index(entity_id, json.dumps(document), public_index, True)
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
                response += '. '
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
            dataset = self._call_entity_api(entity_id=document["dataset_uuid"], endpoint_base="documents")
            return self.is_public(dataset)

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
            entity_visibility = self._call_entity_api(entity_id=document["uuid"], endpoint_base="visibility")
            is_public = entity_visibility == "public"

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
            auth_header_name: auth_scheme + " " + token
        }

        return headers_dict

    # Note: this entity dict input (if Dataset) has already removed ingest_metadata.files and
    # ingest_metadata.metadata sub fields with empty string values from previous call
    def _call_indexer(self, entity: dict):
        logger.info(f"Start executing _call_indexer() on uuid: {entity['uuid']}, entity_type: {entity['entity_type']}")

        try:
            # Generate and write a document for the entity to each index group loaded from the configuration file.
            for index_group in self.indices.keys():
                self._transform_and_write_entity_to_index_group(entity=entity, index_group=index_group)
            logger.info(f"Finished executing _call_indexer() on uuid: {entity['uuid']}, entity_type: {entity['entity_type']}")

        except Exception as e:
            msg = (
                f"Encountered exception e={str(e)} executing _call_indexer() with "
                f"uuid: {entity['uuid']}, entity_type: {entity['entity_type']}"
            )
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def _upsert_index(self, entity_ids: list[str], index: Index, priv_entities: list[dict] = [], pub_entities: list[dict] = []):
        failure_results = {
            index.private: [],
            index.public: []
        }
        for entity_id in entity_ids:
            try:
                # Retrieve the private document
                priv_entity = self._call_entity_api(entity_id=entity_id, endpoint_base="documents", include_token=True)
                priv_entities.append(priv_entity)
            except Exception as e:
                logger.exception(e)
                continue

            if self.is_public(priv_entity):
                try:
                    # Retrieve the public document
                    pub_entity = self._call_entity_api(entity_id=entity_id, endpoint_base="documents", include_token=False)
                    pub_entities.append(pub_entity)
                except Exception:
                    pass

            # Send bulk update when the batch size is reached
            if len(priv_entities) >= self.BULK_UPDATE_SIZE or len(pub_entities) >= self.BULK_UPDATE_SIZE:
                priv_update = BulkUpdate(upserts=priv_entities)
                pub_update = BulkUpdate(upserts=pub_entities)

                failures = self._bulk_update(priv_update, index.private, index.url)
                failure_results[index.private].extend(failures)

                failures = self._bulk_update(pub_update, index.public, index.url)
                failure_results[index.public].extend(failures)

                priv_entities = []
                pub_entities = []

        # Send bulk update for the remaining entities
        if priv_entities:
            priv_update = BulkUpdate(upserts=priv_entities)
            failures = self._bulk_update(priv_update, index.private, index.url)
            failure_results[index.private].extend(failures)
        if pub_entities:
            pub_update = BulkUpdate(upserts=pub_entities)
            failures = self._bulk_update(pub_update, index.public, index.url)
            failure_results[index.public].extend(failures)

        return failure_results

    def _upsert(self, doc: dict, index: str, es_url: str):
        url = f"{es_url}/{index}/_update/{doc['uuid']}"
        body = {
            "doc": doc,
            "doc_as_upsert": True
        }
        response = requests.post(url, json=body, verify=False)
        response.raise_for_status()

    def _bulk_update(self, bulk_update: BulkUpdate, index: str, es_url: str):
        if not bulk_update.upserts and not bulk_update.deletes:
            return []

        url = f"{es_url}/{index}/_bulk"
        headers = {"Content-Type": "application/x-ndjson"}

        # Preparing ndjson content
        upserts = [
            f'{{"update":{{"_id":"{upsert["uuid"]}"}}}}\n{{"doc":{json.dumps(upsert, separators=(",", ":"))},"doc_as_upsert":true}}'
            for upsert in bulk_update.upserts
        ]
        deletes = [f'{{ "delete": {{ "_id": "{delete_uuid}" }} }}' for delete_uuid in bulk_update.deletes]

        body = "\n".join(upserts + deletes) + "\n"
        response = requests.post(url, headers=headers, data=body, verify=False)
        if response.status_code != 200:
            logger.error(f"Failed to bulk update index: {index} in elasticsearch.")
            logger.error(f"Error Message: {response.text}")
            failure_uuids = [upsert["uuid"] for upsert in bulk_update.upserts].extend(bulk_update.deletes)
            return failure_uuids

        res_body = response.json().get("items", [])
        result_values = [
            item.get("update") or item.get("delete")
            for item in res_body
            if "update" in item or "delete" in item
        ]

        failure_uuids = [item["_id"] for item in result_values if item["status"] != 200]
        return failure_uuids

    # This method is supposed to only retrieve Dataset|Source|Sample
    # The Collection and Upload are handled by separate calls
    # The returned data can either be an entity dict or a list of uuids (when `url_property` parameter is specified)
    def _call_entity_api(self, endpoint_base: str, entity_id: Optional[str] = None,
                         endpoint_suffix: Optional[str] = None, url_property: Optional[str] = None,
                         include_token: bool = True):

        logger.info(f"Start executing _call_entity_api() on endpoint_base: {endpoint_base}, uuid: {entity_id}")
        url = f"{self.entity_api_url}/{endpoint_base}"
        if entity_id:
            url = f"{url}/{entity_id}"
        if endpoint_suffix:
            url = f"{url}/{endpoint_suffix}"
        if url_property:
            url = f"{url}?property={url_property}"

        headers = self.request_headers if include_token else None
        response = requests.get(url, headers=headers, verify=False)

        if response.status_code != 200:
            msg = f"_call_entity_api() failed on endpoint_base: {endpoint_base}, uuid: {entity_id}"

            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            logger.debug(f"======_call_entity_api() status code from entity-api: {response.status_code}======")
            logger.debug("======_call_entity_api() response text from entity-api======")
            logger.debug(response.text)

            # Add this uuid to the failed list
            self.failed_entity_api_calls.append(url)
            self.failed_entity_ids.append(entity_id)

            # Bubble up the error message from entity-api instead of sys.exit(msg)
            # The caller will need to handle this exception
            response.raise_for_status()
            raise requests.exceptions.RequestException(response.text)

        logger.info(f"Finished executing _call_entity_api() on endpoint_base: {endpoint_base}, uuid: {entity_id}")

        # The resulting data can be an entity dict or a list (when `url_property` parameter is specified)
        # For Dataset, data manipulation is performed
        # If result is a list or not a Dataset dict, no change - 7/13/2022 Max & Zhou
        return response.json()

    def _delete_index(self, index: str):
        try:
            self.indexer.delete_index(index)
        except Exception:
            pass


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
        instance_path=os.path.join(
            os.path.abspath(os.path.dirname(__file__)), "../src/instance"
        ),
        instance_relative_config=True,
    )
    app.config.from_pyfile("app.cfg")
    ubkg_instance = initialize_ubkg(app.config)

    INDICES = safe_load(
        (Path(__file__).absolute().parent / "instance/search-config.yaml").read_text()
    )

    try:
        token = sys.argv[1]
    except IndexError:
        msg = "Missing admin group token argument"
        logger.exception(msg)
        sys.exit(msg)

    # Create an instance of the indexer
    translator = Translator(
        INDICES,
        app.config["APP_CLIENT_ID"],
        app.config["APP_CLIENT_SECRET"],
        token,
        ubkg_instance,
    )

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

    translator.delete_and_recreate_indices(files=False)
    translator.delete_and_recreate_indices(files=True)
    translator.translate_all()

    # Show the failed entity-api calls and the uuids
    if translator.failed_entity_api_calls:
        logger.info(f"{len(translator.failed_entity_api_calls)} entity-api calls failed")
        print(*translator.failed_entity_api_calls, sep="\n")

    if translator.failed_entity_ids:
        logger.info(f"{len(translator.failed_entity_ids)} entity ids failed")
        print(*translator.failed_entity_ids, sep="\n")

    end = time.time()
    logger.info(f"############# Full index via script completed. Total time used: {end - start} seconds. #############")
