import concurrent.futures
import copy
import json
import logging
import os
import re
import sys
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union

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
from translator.tranlation_helper_functions import (
    get_all_reindex_enabled_indice_names,
    get_uuids_by_entity_type,
    remove_specific_key_entry,
)
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

    entity_api_cache = {}
    ontology_lookup_cache = {}
    sparql_vocabs = {
        "purl.obolibrary.org": "uberon",
        "purl.org": "fma",
    }

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

        self.request_headers = self.create_request_headers_for_auth(token)
        self.entity_api_url = self.indices[self.DEFAULT_INDEX_WITHOUT_PREFIX]["document_source_endpoint"].strip("/")

        # Add index_version by parsing the VERSION file
        self.index_version = ((Path(__file__).absolute().parent.parent / 'VERSION').read_text()).strip()

    def __get_scope_list(self, entity_id, document, index, scope):
        scope_list = []
        if index == "files":
            # It would be nice if the possible scopes could be identified from
            # self.INDICES['indices'] rather than hardcoded. @TODO
            # This can handle indices besides "files" which might accept "scope" as
            # an argument, but returning an empty list, not raising an Exception, for
            # an unrecognized index name.
            if scope is not None:
                if scope not in ["public", "private"]:
                    msg = (
                        f"Unrecognized scope '{scope}' requested for "
                        f"entity_id '{entity_id}' in Dataset '{document['dataset_uuid']}."
                    )
                    logger.info(msg)
                    raise ValueError(msg)

                elif scope == "public":
                    if self.is_public(document):
                        scope_list.append(scope)
                    else:
                        # Reject the addition of 'public' was explicitly indicated, even though
                        # the public index may be silently skipped when a scope is not specified, in
                        # order to mimic behavior below for "non-self managed" indices.
                        msg = (
                            f"Dataset '{document['dataset_uuid']} "
                            f"does not have status {self.DATASET_STATUS_PUBLISHED}, so "
                            f"entity_id '{entity_id}' cannot go in a public index."
                        )
                        logger.info(msg)
                        raise ValueError(msg)

                elif scope == "private":
                    scope_list.append(scope)
            else:
                scope_list = ["public", "private"]

        return scope_list

    def translate_all(self):
        with app.app_context():
            try:
                logger.info("############# Reindex Live Started #############")
                start = time.time()

                source_uuids_list = get_uuids_by_entity_type("source", self.request_headers, self.DEFAULT_ENTITY_API_URL)
                upload_uuids_list = get_uuids_by_entity_type("upload", self.request_headers, self.DEFAULT_ENTITY_API_URL)
                collection_uuids_list = get_uuids_by_entity_type("collection", self.request_headers, self.DEFAULT_ENTITY_API_URL)

                # Only need this comparison for the live /reindex-all PUT call
                if not self.skip_comparison:
                    # Make calls to entity-api to get a list of uuids for rest of entity types
                    sample_uuids_list = get_uuids_by_entity_type("sample", self.request_headers, self.DEFAULT_ENTITY_API_URL)
                    dataset_uuids_list = get_uuids_by_entity_type("dataset", self.request_headers, self.DEFAULT_ENTITY_API_URL)

                    # Merge into a big list that with no duplicates
                    all_entities_uuids = set(
                        source_uuids_list
                        + sample_uuids_list
                        + dataset_uuids_list
                        + upload_uuids_list
                        + collection_uuids_list
                    )

                    es_uuids = []
                    index_names = get_all_reindex_enabled_indice_names(self.INDICES)

                    for index in index_names.keys():
                        all_indices = index_names[index]
                        # get URL for that index
                        es_url = self.INDICES["indices"][index]["elasticsearch"]["url"].strip("/")

                        for actual_index in all_indices:
                            es_uuids.extend(get_uuids_from_es(actual_index, es_url))

                    es_uuids = set(es_uuids)

                    # Remove entities found in Elasticsearch but no longer in neo4j
                    for uuid in es_uuids:
                        if uuid not in all_entities_uuids:
                            logger.debug(
                                f"Entity of uuid: {uuid} found in Elasticsearch but no "
                                "longer in neo4j. Delete it from Elasticsearch."
                            )
                            self.delete(uuid)

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    # The default number of threads in the ThreadPoolExecutor is calculated as:
                    # From 3.8 onwards default value is min(32, os.cpu_count() + 4)
                    # Where the number of CPUs is determined by Python and will take hyperthreading into account
                    logger.info(f"The number of worker threads being used by default: {executor._max_workers}")

                    # Submit tasks to the thread pool
                    collection_futures_list = [
                        executor.submit(self.translate_collection, uuid, reindex=True)
                        for uuid in collection_uuids_list
                    ]
                    upload_futures_list = [
                        executor.submit(self.translate_upload, uuid, reindex=True)
                        for uuid in upload_uuids_list
                    ]

                    # Append the above lists into one
                    futures_list = collection_futures_list + upload_futures_list

                    # The target function runs the task logs more details when f.result() gets executed
                    for f in concurrent.futures.as_completed(futures_list):
                        _ = f.result()

                # Index the source tree in a regular for loop, not the concurrent mode
                # However, the descendants of a given source will be indexed concurrently
                for uuid in source_uuids_list:
                    self.translate_source_tree(uuid)

                end = time.time()

                logger.info(f"Finished executing translate_all(). Total time used: {end - start} seconds.")

            except Exception as e:
                logger.error(e)

    def translate(self, entity_id):
        try:
            # Retrieve the entity details
            # This returned entity dict (if Dataset) has removed ingest_metadata.files and
            # ingest_metadata.metadata sub fields with empty string values when call_entity_api() gets called
            entity = self.call_entity_api(entity_id=entity_id, endpoint_base="documents")

            logger.info(f"Start executing translate() on {entity['entity_type']} of uuid: {entity_id}")

            if entity["entity_type"] == "Collection":
                # Expect entity-api to stop update of Collections which should not be modified e.g. those which
                # have a DOI. But entity-api may still request such Collections be indexed, particularly right
                # after the Collection becomes visible to the public.
                try:
                    self.translate_collection(entity_id, reindex=True)
                except Exception as e:
                    logger.error(f"Unable to index {entity['entity_type']} due to e={str(e)}")

            elif equals(entity["entity_type"], self.entities.UPLOAD):
                self.translate_upload(entity_id, reindex=True)

            else:
                # For newly created entities and entities whose relationships in Neo4j have changed since the
                # entity was indexed into OpenSearch, use "reindex" code to bring the OpenSearch document
                # up-to-date for the entity and all the entities it relates to.
                #
                # For entities previously indexed into OpenSearch whose relationships in Neo4j have not changed,
                # just index the document for the entity. Then update fields belong to related entities which
                # refer to the entity i.e. the 'ancestors' list of this entity's 'descendants', the 'descendants'
                # list of this entity's 'ancestors', etc.

                # get URL for the OpenSearch server
                es_url = self.INDICES["indices"]["entities"]["elasticsearch"]["url"].strip("/")

                # Get the ancestors and descendants of this entity as they exist in Neo4j, and as they
                # exist in OpenSearch.
                neo4j_ancestor_ids = self.call_entity_api(
                    entity_id=entity_id,
                    endpoint_base="ancestors",
                    url_property="uuid",
                )
                neo4j_descendant_ids = self.call_entity_api(
                    entity_id=entity_id,
                    endpoint_base="descendants",
                    url_property="uuid",
                )

                # If working with a Dataset, it may be copied into ElasticSearch documents for
                # Collections and Uploads, so identify any of those which must be reindexed.
                neo4j_collection_ids = []
                neo4j_upload_ids = []
                if entity["entity_type"] == "Dataset":
                    neo4j_collection_ids = self.call_entity_api(
                        entity_id=entity_id,
                        endpoint_base="entities",
                        endpoint_suffix="collections",
                        url_property="uuid",
                    )
                    neo4j_upload_ids = self.call_entity_api(
                        entity_id=entity_id,
                        endpoint_base="entities",
                        endpoint_suffix="uploads",
                        url_property="uuid",
                    )

                # Use the index with documents for all entities to determine the relationships of the
                # current entity as stored in OpenSearch.  Consider it safe to assume documents in other
                # indices for the same entity have exactly the same relationships unless there was an
                # indexing problem.
                #
                # "Changed relationships" only applies to differences in the ancestors and descendants of
                # an entity. Uploads and Collections which reference a Dataset entity, for example, do not
                # indicate a change of relationships which would result in reindexing instead of directly updating.
                index_with_everything = self.INDICES["indices"]["entities"]["private"]
                existing_entity_json = self._get_existing_entity_relationships(
                    entity_uuid=entity["uuid"],
                    es_url=es_url,
                    es_index=index_with_everything,
                )
                relationships_changed = self._relationships_changed_since_indexed(
                    neo4j_ancestor_ids=neo4j_ancestor_ids,
                    neo4j_descendant_ids=neo4j_descendant_ids,
                    existing_oss_doc=existing_entity_json,
                )

                # Now that it has been determined whether relationships have changed for this entity,
                # reindex the entity itself first before dealing with other documents for related entities.
                self._call_indexer(entity=entity, delete_existing_doc_first=True)

                if relationships_changed:
                    logger.info(f"Related entities for {entity_id} have changed in Neo4j. Reindexing")

                    # Since the entity is new or the Neo4j relationships with related entities have changed,
                    # reindex the current entity
                    self._reindex_related_entities(
                        entity_id=entity_id,
                        entity_type=entity["entity_type"],
                        neo4j_ancestor_ids=neo4j_ancestor_ids,
                        neo4j_descendant_ids=neo4j_descendant_ids,
                        neo4j_collection_ids=neo4j_collection_ids,
                        neo4j_upload_ids=neo4j_upload_ids,
                    )
                else:
                    logger.info(
                        f"Related entities for {entity_id} are unchanged in Neo4j. "
                        "Directly updating index docs of related entities."
                    )

                    # Since the entity's relationships are identical in Neo4j and OpenSearch, just update
                    # documents in the entities indices with a copy of the current entity.
                    for es_index in [
                        self.INDICES["indices"]["entities"]["private"],
                        self.INDICES["indices"]["entities"]["public"],
                    ]:
                        # Since _directly_modify_related_entities() will only _update documents which already
                        # exist in an index, no need to test if this entity belongs in the public index.
                        self._directly_modify_related_entities(
                            es_url=es_url,
                            es_index=es_index,
                            entity_id=entity_id,
                            neo4j_ancestor_ids=neo4j_ancestor_ids,
                            neo4j_descendant_ids=neo4j_descendant_ids,
                            neo4j_collection_ids=neo4j_collection_ids,
                            neo4j_upload_ids=neo4j_upload_ids,
                        )

                logger.info(
                    f"Finished executing translate() on {entity['entity_type']} of uuid: {entity_id}"
                )

        except Exception as e:
            msg = f"Exceptions during executing translate(): {e}"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def update(self, entity_id: str, document: dict, index: str = None, scope: str = None):
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
                response += self.indexer.index(
                    entity_id, json.dumps(document), target_index, True
                )
                response += ". "

        else:
            for index in self.indices.keys():
                public_index = self.INDICES["indices"][index]["public"]
                private_index = self.INDICES["indices"][index]["private"]

                if self.is_public(document):
                    response = self.indexer.index(entity_id, json.dumps(document), public_index, True)

                response += self.indexer.index(entity_id, json.dumps(document), private_index, True)

        return response

    def add(self, entity_id: str, document: dict, index: str = None, scope: str = None):
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
                response += self.indexer.index(entity_id, json.dumps(document), target_index, False)
                response += ". "

        else:
            for index in self.indices.keys():
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
            dataset = self.call_entity_api(entity_id=document["dataset_uuid"], endpoint_base="documents")
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
            entity_visibility = self.call_entity_api(entity_id=document["uuid"], endpoint_base="visibility")
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

    def delete_docs(self, index: str, scope: str, entity_id: str):
        # Clear multiple documents from the OpenSearch indices associated with the composite index specified
        # When index is for the files-api and entity_id is for a File, clear all file manifests for the File.
        # When index is for the files-api and entity_id is for a Dataset, clear all file manifests for the Dataset.
        # When index is for the files-api and entity_id is not specified, clear all file manifests in the index.
        # Otherwise, raise an Exception indicating the specified arguments are not supported.

        if not index:
            # Shouldn't happen due to configuration of Flask Blueprint routes
            raise ValueError("index must be specified for delete_docs()")

        if index == "files":
            # For deleting documents, try removing them from the specified scope, but do not
            # raise any Exception or return an error response if they are not there to be deleted.
            scope_list = [scope] if scope else ["public", "private"]

            if entity_id:
                try:
                    # Get the Dataset entity with the specified entity_id
                    entity = self.call_entity_api(entity_id=entity_id, endpoint_base="documents")
                except Exception:
                    # entity-api may throw an Exception if entity_id is actually the
                    # uuid of a File, so swallow the error here and process as
                    # removing the file info document for a File below
                    logger.info(
                        f"No entity found  with entity_id '{entity_id}' in Neo4j, so process as "
                        "a request to delete a file info document for a File with that UUID."
                    )
                    entity = {"entity_type": "File", "uuid": entity_id}

            response = ""
            for scope in scope_list:
                target_index = self.self_managed_indices[index][scope]
                if entity_id:
                    # Confirm the found entity for entity_id is of a supported type.  This probably repeats
                    # work done by the caller, but count on the caller for other business logic, like constraining
                    # to Datasets without PHI.
                    if entity and entity["entity_type"] not in ["Dataset", "Publication", "File"]:
                        raise ValueError(
                            "Translator.delete_docs() is not configured to clear documents for "
                            f"entities of type '{entity['entity_type']} for HuBMAP."
                        )

                    elif entity["entity_type"] in ["Dataset", "Publication"]:
                        try:
                            resp = self.indexer.delete_fieldmatch_document(target_index, "dataset_uuid", entity["uuid"])
                            response += resp[0]
                        except Exception as e:
                            response += (
                                f"While deleting the Dataset '{entity['uuid']}' file info documents"
                                f"from {target_index}, exception raised was {str(e)}."
                            )

                    elif entity["entity_type"] == "File":
                        try:
                            resp = self.indexer.delete_fieldmatch_document(target_index, "file_uuid", entity["uuid"])
                            response += resp[0]
                        except Exception as e:
                            response += (
                                f"While deleting the File '{entity['uuid']}' file info document "
                                f"from {target_index}, exception raised was {str(e)}."
                            )

                    else:
                        raise ValueError(
                            f"Unable to find a Dataset or File with identifier {entity_id} whose "
                            "file info documents can be deleted from OpenSearch."
                        )
                else:
                    # Since a File or a Dataset was not specified, delete all documents from
                    # the target index.
                    response += self.indexer.delete_fieldmatch_document(target_index)
                response += " "
            return response

        else:
            raise ValueError(f"The index '{index}' is not recognized for delete_docs() operations.")

    def delete(self, entity_id: str):
        for index, _ in self.indices.items():
            # each index should have a public/private index
            public_index = self.INDICES["indices"][index]["public"]
            self.indexer.delete_document(entity_id, public_index)

            private_index = self.INDICES["indices"][index]["private"]
            if public_index != private_index:
                self.indexer.delete_document(entity_id, private_index)

    # When indexing, Upload WILL NEVER BE PUBLIC
    def translate_upload(self, entity_id: str, reindex: bool = False):
        try:
            logger.info(f"Start executing translate_upload() for {entity_id}")
            default_private_index = self.INDICES["indices"][self.DEFAULT_INDEX_WITHOUT_PREFIX]["private"]

            # Retrieve the upload entity details
            upload = self.call_entity_api(entity_id=entity_id, endpoint_base="documents")

            self.add_datasets_to_entity(upload)
            self._entity_keys_rename(upload)

            # Add additional calculated fields if any applies to Upload
            self.add_calculated_fields(upload)

            self._index_doc_directly_to_es_index(
                entity=upload,
                document=json.dumps(upload),
                es_index=default_private_index,
                delete_existing_doc_first=reindex,
            )
            logger.info(f"Finished executing translate_upload() for {entity_id}")
        except Exception as e:
            logger.error(e)

    def translate_collection(self, entity_id: str, reindex: bool = False):
        logger.info(f"Start executing translate_collection() for {entity_id}")

        # The entity-api returns public collection with a list of connected public/published datasets, for either
        # - a valid token but not in HuBMAP-Read group or
        # - no token at all
        # Here we do NOT send over the token
        try:
            collection = self.get_collection_doc(entity_id=entity_id)

            self.add_datasets_to_entity(collection)
            self._entity_keys_rename(collection)

            # Add additional calculated fields if any applies to Collection
            self.add_calculated_fields(collection)

            for index in self.indices.keys():
                # each index should have a public index
                public_index = self.INDICES["indices"][index]["public"]
                private_index = self.INDICES["indices"][index]["private"]

                # if the index has a transformer use that else do a now load
                if self.TRANSFORMERS.get(index):
                    json_data = json.dumps(
                        self.TRANSFORMERS[index].transform(collection, self.transformation_resources)
                    )
                else:
                    json_data = json.dumps(collection)

                # If this Collection meets entity-api's criteria for visibility to the world by
                # returning the value of its schema_constants.py DataVisibilityEnum.PUBLIC, put
                # the Collection in the public index.
                if self.is_public(collection):
                    self._index_doc_directly_to_es_index(
                        entity=collection,
                        document=json_data,
                        es_index=public_index,
                        delete_existing_doc_first=reindex,
                    )

                self._index_doc_directly_to_es_index(
                    entity=collection,
                    document=json_data,
                    es_index=private_index,
                    delete_existing_doc_first=reindex,
                )

            logger.info(f"Finished executing translate_collection() for {entity_id}")

        except requests.exceptions.RequestException as e:
            logger.exception(e)
            # Log the error and will need fix later and reindex, rather than sys.exit()
            logger.error(f"translate_collection() failed to get collection of uuid: {entity_id} via entity-api")

        except Exception as e:
            logger.error(e)

    def translate_source_tree(self, entity_id: str):
        try:
            logger.info(f"Start executing translate_source_tree() for source of uuid: {entity_id}")

            descendant_uuids = self.call_entity_api(
                entity_id=entity_id,
                endpoint_base="descendants",
                url_property="uuid",
            )
            # Index the source entity itself
            source = self.call_entity_api(entity_id=entity_id, endpoint_base="documents")
            self._call_indexer(entity=source)

            # Index all the descendants of this source
            with concurrent.futures.ThreadPoolExecutor() as executor:
                source_descendants_list = [
                    executor.submit(self.index_entity, uuid)
                    for uuid in descendant_uuids
                ]
                for f in concurrent.futures.as_completed(source_descendants_list):
                    _ = f.result()

            logger.info(f"Finished executing translate_source_tree() for source of uuid: {entity_id}")

        except Exception as e:
            logger.error(e)

    def init_auth_helper(self):
        if AuthHelper.isInitialized() is False:
            auth_helper = AuthHelper.create(self.app_client_id, self.app_client_secret)
        else:
            auth_helper = AuthHelper.instance()

        return auth_helper

    # Create a dict with HTTP Authorization header with Bearer token
    def create_request_headers_for_auth(self, token: str):
        auth_header_name = "Authorization"
        auth_scheme = "Bearer"

        headers_dict = {
            # Don't forget the space between scheme and the token value
            auth_header_name: auth_scheme + " " + token
        }

        return headers_dict

    # Note: this entity dict input (if Dataset) has already removed ingest_metadata.files and
    # ingest_metadata.metadata sub fields with empty string values from previous call
    def _call_indexer(self, entity: dict, delete_existing_doc_first: bool = False):
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

    # Used for Upload and Collection index
    def add_datasets_to_entity(self, entity: dict):
        logger.info("Start executing add_datasets_to_entity()")

        datasets = []
        if "datasets" in entity:
            for dataset in entity["datasets"]:
                # Retrieve the entity details
                try:
                    dataset = self.call_entity_api(entity_id=dataset["uuid"], endpoint_base="documents")
                except Exception as e:
                    logger.exception(e)
                    logger.error(
                        f"Failed to retrieve dataset {dataset['uuid']} via entity-api during "
                        "executing add_datasets_to_entity(), skip and continue to next one"
                    )

                    # This can happen when the dataset is in neo4j but the actual uuid is not found in MySQL
                    # or somehting else is wrong with entity-api and it can't return the dataset info
                    # In this case, we'll skip over the current iteration, and continue with the next one
                    # Otherwise, null will be added to the  resuting datasets list and break portal-ui rendering - 5/3/2023 Zhou
                    continue

                try:
                    dataset_doc = self.generate_doc(dataset, "dict")
                except Exception as e:
                    logger.exception(e)
                    logger.error(
                        f"Failed to execute generate_doc() on dataset {dataset['uuid']} during "
                        "executing add_datasets_to_entity(), skip and continue to next one"
                    )

                    # This can happen when the dataset itself is good but something failed to generate the doc
                    # E.g., one of the descendants of this dataset exists in neo4j but no record in uuid MySQL
                    # In this case, we'll skip over the current iteration, and continue with the next one
                    # Otherwise, no document is generated, null will be added to the resuting datasets list and break portal-ui rendering - 5/3/2023 Zhou
                    continue

                self.exclude_added_top_level_properties(dataset_doc, except_properties_list=["files", "datasets"])
                datasets.append(dataset_doc)

        entity["datasets"] = datasets

        logger.info("Finished executing add_datasets_to_entity()")

    # Given a dictionary for an entity containing Neo4j and entity-api-generated data, assume all entries
    # are to be used in OpenSearch documents. Modify any key names specified to change, and return a dictionary of
    # all names and values to store in an OpenSearch document.
    def _entity_keys_rename(self, entity: dict):
        global neo4j_to_es_attribute_name_map

        logger.info("Start executing _entity_keys_rename()")

        for remapped_key in neo4j_to_es_attribute_name_map:
            if remapped_key not in entity:
                continue

            attribute_value = entity[remapped_key]
            # Since statement above assures remapped key is in entity, and the value
            # is captured, delete the dict entry keyed by the Neo4j name.
            del entity[remapped_key]
            # Restore the value to a dict entry keyed by the OpenSearch name.
            entity[neo4j_to_es_attribute_name_map[remapped_key]] = attribute_value

        # Special case of Sample.rui_location
        # To be backward compatible for API clients relying on the old version
        # Also gives the ES consumer flexibility to change the inner structure
        # Note: when `rui_location` is stored as json object (Python dict) in ES
        # with the default dynamic mapping, it can cause errors due to
        # the changing data types of some internal fields
        # isinstance() check is to avoid json.dumps() on json string again
        if "rui_location" in entity and isinstance(entity["rui_location"], dict):
            entity["rui_location"] = json.dumps(entity["rui_location"])

        logger.info("Finished executing _entity_keys_rename()")

    def _get_ontology_label(self, ann_url: str) -> Optional[str]:
        """Get the label from the appropriate ontology lookup service.

        Args:
            ann_url (str): The annotation url.

        Returns:
            Optional[dict]: The label and purl if found, otherwise None.
        """
        if ann_url in self.ontology_lookup_cache:
            return {"label": self.ontology_lookup_cache[ann_url], "purl": ann_url}

        host = urllib.parse.urlparse(ann_url).hostname
        vocab = self.sparql_vocabs.get(host)
        if not vocab:
            return None

        schema = "http://www.w3.org/2000/01/rdf-schema#label"
        table = f"https://purl.humanatlas.io/vocab/{vocab}"
        query = f"SELECT ?label FROM <{table}> WHERE {{ <{ann_url}> <{schema}> ?label }}"
        headers = {
            "Accept": "application/sparql-results+json",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        res = requests.post("https://lod.humanatlas.io/sparql", data={"query": query}, headers=headers)
        if res.status_code != 200:
            return None

        bindings = res.json().get("results", {}).get("bindings", [])
        if len(bindings) != 1:
            return None

        label = bindings[0].get("label", {}).get("value")
        if not label:
            return None

        self.ontology_lookup_cache[ann_url] = label  # cache the result
        return {"label": label, "purl": ann_url}

    # These calculated fields are not stored in neo4j but will be generated
    # and added to the ES
    def add_calculated_fields(self, entity: dict):
        # Add index_version by parsing the VERSION file
        entity["index_version"] = self.index_version

        # Add display_subtype
        if entity["entity_type"] in self.entity_types:
            entity["display_subtype"] = self.generate_display_subtype(entity)

        # Add has rui information for all non-organ samples and datasets
        if (entity["entity_type"] in ["Sample", "Dataset"]
                and entity.get("sample_category") != "Organ"
                and entity.get("source", {}).get("source_type") in ["Human", "Human Organoid"]):

            ancestors = entity.get("ancestors", [])
            organs = set([a["organ"] for a in ancestors if "organ" in a])
            if len(organs.intersection({"AD", "BD", "BM", "BS", "MU", "OT"})) > 0:
                # Has an organ ancestor that is not supported
                entity["has_rui_information"] = "N/A"
            else:
                has_rui = (
                    "rui_location" in entity
                    or len([a for a in ancestors if "rui_location" in a]) > 0
                )
                entity["has_rui_information"] = str(has_rui)

        # Add rui information anatomical location
        if "rui_location" in entity:
            rui_location = json.loads(entity["rui_location"])
            if "ccf_annotations" in rui_location:
                annotation_urls = rui_location["ccf_annotations"]
                labels = [
                    label
                    for url in annotation_urls
                    if (label := self._get_ontology_label(url))
                ]
                if len(labels) > 0:
                    entity["rui_location_anatomical_locations"] = labels

        # Add last touch
        last_touch = (
            entity["published_timestamp"]
            if "published_timestamp" in entity
            else entity["last_modified_timestamp"]
        )
        timestamp = str(datetime.fromtimestamp(last_touch / 1000, tz=timezone.utc))
        entity["last_touch"] = timestamp.split("+")[0]

        # Add dataset category
        if entity["entity_type"] == "Dataset" and "creation_action" in entity:
            dataset_category_map = {
                "Create Dataset Activity": "primary",
                "Multi-Assay Split": "component",
                "Central Process": "codcc-processed",
                "Lab Process": "lab-processed",
            }
            if dataset_category := dataset_category_map.get(entity["creation_action"]):
                entity["dataset_category"] = dataset_category

    # For Upload, Dataset, Source and Sample objects:
    # add a calculated (not stored in Neo4j) field called `display_subtype` to
    # all Elasticsearch documents of the above types with the following rules:
    # Upload: Just make it "Data Upload" for all uploads
    # Source: "Source"
    # Sample: if sample_category == 'organ' the display name linked to the corresponding description of organ code
    # otherwise the display name linked to the value of the corresponding description of sample_category code
    def generate_display_subtype(self, entity: dict):
        entity_type = entity["entity_type"]
        display_subtype = "{unknown}"

        if equals(entity_type, self.entities.SOURCE):
            display_subtype = entity["source_type"]

        elif equals(entity_type, self.entities.SAMPLE):
            if "sample_category" in entity:
                if equals(entity["sample_category"], Ontology.ops().specimen_categories().ORGAN):
                    if "organ" in entity:
                        organ_types = Ontology.ops(as_data_dict=True, prop_callback=None, key="rui_code", val_key="term").organ_types()
                        organ_types["OT"] = "Other"
                        display_subtype = get_val_by_key(entity["organ"], organ_types, "ubkg.organ_types")
                    else:
                        logger.error(
                            "Missing missing organ when sample_category is set "
                            f"of Sample with uuid: {entity['uuid']}"
                        )

                else:
                    sample_categories = Ontology.ops(as_data_dict=True, prop_callback=None).specimen_categories()
                    display_subtype = get_val_by_key(entity["sample_category"], sample_categories, "ubkg.specimen_categories")

            else:
                logger.error(f"Missing sample_category of Sample with uuid: {entity['uuid']}")

        elif equals(entity_type, self.entities.DATASET):
            if "dataset_type" in entity:
                display_subtype = entity["dataset_type"]
            else:
                logger.error(f"Missing dataset_type of Dataset with uuid: {entity['uuid']}")

        elif equals(entity_type, self.entities.UPLOAD):
            display_subtype = "Data Upload"

        else:
            # Do nothing
            logger.error(
                f"Invalid entity_type: {entity_type}. "
                "Only generate display_subtype for Source/Sample/Dataset/Upload"
            )

        logger.info("Finished executing generate_display_subtype()")
        return display_subtype

    def generate_doc(self, entity: dict, return_type):
        try:
            entity_id = entity["uuid"]

            if not equals(entity["entity_type"], self.entities.UPLOAD):
                ancestors = []
                descendants = []
                ancestor_ids = []
                descendant_ids = []
                immediate_ancestors = []
                immediate_descendants = []

                # Do not call /ancestors/<id> directly to avoid performance/timeout issue
                ancestor_ids = self.call_entity_api(
                    entity_id=entity_id,
                    endpoint_base="ancestors",
                    url_property="uuid"
                )
                for ancestor_uuid in ancestor_ids:
                    # Retrieve the entity details
                    ancestor_dict = self.call_entity_api(entity_id=ancestor_uuid, endpoint_base="documents")
                    ancestor_dict.pop("pipeline_message", None)
                    # Add to the list
                    ancestors.append(ancestor_dict)

                descendant_ids = self.call_entity_api(
                    entity_id=entity_id,
                    endpoint_base="descendants",
                    url_property="uuid"
                )
                for descendant_uuid in descendant_ids:
                    # Retrieve the entity details
                    descendant_dict = self.call_entity_api(entity_id=descendant_uuid, endpoint_base="documents")
                    descendant_dict.pop("pipeline_message", None)
                    # Add to the list
                    descendants.append(descendant_dict)

                # Calls to /parents/<id> and /children/<id> have no performance/timeout concerns
                immediate_ancestors = self.call_entity_api(entity_id=entity_id, endpoint_base="parents")
                immediate_descendants = self.call_entity_api(entity_id=entity_id, endpoint_base="children")

                # Add new properties to entity
                entity["ancestors"] = ancestors
                entity["descendants"] = descendants

                entity["ancestor_ids"] = ancestor_ids
                entity["descendant_ids"] = descendant_ids

                # TODO: remove all instances of this pipeline_message deletion when new feature to skip properties
                # within GET entity of entity-api becomes available
                for immediate_ancestor in immediate_ancestors:
                    immediate_ancestor.pop("pipeline_message", None)

                for immediate_descendant in immediate_descendants:
                    immediate_descendant.pop("pipeline_message", None)

                entity["immediate_ancestors"] = immediate_ancestors
                entity["immediate_descendants"] = immediate_descendants

            # The origin_sample is the sample that `sample_category` is "organ" and the `organ` code is set at the same time
            if entity["entity_type"] in ["Sample", "Dataset", "Publication"]:
                sample_categories = Ontology.ops().specimen_categories()

                entity["origin_sample"] = None
                if ("sample_category" in entity
                        and "organ" in entity
                        and entity["organ"].strip() != ""
                        and equals(entity["sample_category"], sample_categories.ORGAN)):
                    entity["origin_sample"] = copy.copy(entity)

                if entity["origin_sample"] is None:
                    try:
                        # The origin_sample is the ancestor which `sample_category` is "organ" and the `organ` code is set
                        itr = next(
                            a for a in ancestors
                            if ("sample_category" in a
                                and "organ" in a
                                and a["organ"].strip() != ""
                                and equals(a["sample_category"], sample_categories.ORGAN))
                        )
                        entity["origin_sample"] = copy.copy(itr)
                    except StopIteration:
                        entity["origin_sample"] = {}

                # Trying to understand here!!!
                if entity["entity_type"] in ["Dataset", "Publication"]:

                    # Reduce pipeline_message when it exceeds 32766 bytes
                    if "pipeline_message" in entity:
                        max_bytes = 32766
                        msg_byte_array = bytearray(entity["pipeline_message"], "utf-8")
                        if len(msg_byte_array) > max_bytes:
                            max_bytes_msg = msg_byte_array[: (max_bytes - 1)]
                            entity["pipeline_message"] = max_bytes_msg.decode("utf-8")

                    # Move files to the root level if exist
                    if "ingest_metadata" in entity and equals(entity["entity_type"], self.entities.DATASET):
                        # Because we remove files from metadata later (to reduce size) we need to shallow copy of metadata
                        metadata = copy.copy(entity["ingest_metadata"])
                        if metadata is not None and "files" in metadata:
                            entity["files"] = metadata["files"]
                            entity["ingest_metadata"].pop("files", None)

                    # Add multi-revisions
                    if "next_revision_uuid" in entity or "previous_revision_uuid" in entity:
                        multi_revisions = self.get_multi_revisions(entity_id, True)
                        if multi_revisions:
                            entity["multi_revisions"] = multi_revisions

            self._entity_keys_rename(entity)

            if entity.get("origin_sample", None):
                self._entity_keys_rename(entity["origin_sample"])
            if entity.get("source_sample", None):
                for s in entity.get("source_sample", None):
                    self._entity_keys_rename(s)
            if entity.get("ancestors", None):
                for a in entity.get("ancestors", None):
                    self._entity_keys_rename(a)
            if entity.get("descendants", None):
                for d in entity.get("descendants", None):
                    self._entity_keys_rename(d, True)
            if entity.get("immediate_descendants", None):
                for parent in entity.get("immediate_descendants", None):
                    self._entity_keys_rename(parent, True)
            if entity.get("immediate_ancestors", None):
                for child in entity.get("immediate_ancestors", None):
                    self._entity_keys_rename(child, True)

            remove_specific_key_entry(entity, "other_metadata")

            # Add additional calculated fields
            self.add_calculated_fields(entity)

            return json.dumps(entity) if return_type == "json" else entity

        except Exception:
            # Log the full stack trace, prepend a line with our message
            msg = "Exceptions during executing indexer.generate_doc()"
            logger.exception(msg)

    def generate_public_doc(self, entity: dict):
        # Only Dataset has this 'next_revision_uuid' property
        property_key = "next_revision_uuid"
        if entity["entity_type"] in ["Dataset", "Publication"] and property_key in entity:
            next_revision_uuid = entity[property_key]
            # Making a call against entity-api/entities/<next_revision_uuid>?property=status
            url = (
                self.entity_api_url
                + "/entities/"
                + next_revision_uuid
                + "?property=status"
            )
            response = requests.get(url, headers=self.request_headers, verify=False)

            if response.status_code != 200:
                msg = (
                    "indexer.generate_public_doc() failed to get status of "
                    f"next_revision_uuid via entity-api for uuid: {next_revision_uuid}"
                )
                logger.error(msg)
                sys.exit(msg)

            # The call to entity-api returns string directly
            dataset_status = (response.text).lower()

            # Check the `next_revision_uuid` and if the dataset is not published,
            # pop the `next_revision_uuid` from this entity
            if dataset_status != self.DATASET_STATUS_PUBLISHED:
                logger.debug(f"Remove the {property_key} property from {entity['uuid']}")
                entity.pop(property_key)

        entity["descendants"] = list(filter(self.is_public, entity["descendants"]))
        entity["immediate_descendants"] = list(filter(self.is_public, entity["immediate_descendants"]))
        return json.dumps(entity)

    # This method is supposed to only retrieve Dataset|Source|Sample
    # The Collection and Upload are handled by separate calls
    # The returned data can either be an entity dict or a list of uuids (when `url_property` parameter is specified)
    def call_entity_api(self, entity_id: str, endpoint_base: str,
                        endpoint_suffix: Optional[str] = None, url_property: Optional[str] = None):

        logger.info(f"Start executing call_entity_api() on uuid: {entity_id}")

        url = f"{self.entity_api_url}/{endpoint_base}/{entity_id}"
        if endpoint_suffix:
            url = f"{url}/{endpoint_suffix}"
        if url_property:
            url = f"{url}?property={url_property}"

        response = requests.get(url, headers=self.request_headers, verify=False)

        if response.status_code != 200:
            msg = f"call_entity_api() failed to get entity of uuid {entity_id} via entity-api"

            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            logger.debug(f"======call_entity_api() status code from entity-api: {response.status_code}======")
            logger.debug("======call_entity_api() response text from entity-api======")
            logger.debug(response.text)

            # Add this uuid to the failed list
            self.failed_entity_api_calls.append(url)
            self.failed_entity_ids.append(entity_id)

            # Bubble up the error message from entity-api instead of sys.exit(msg)
            # The caller will need to handle this exception
            response.raise_for_status()
            raise requests.exceptions.RequestException(response.text)

        logger.info(f"Finished executing call_entity_api() on uuid: {entity_id}")

        # The resulting data can be an entity dict or a list (when `url_property` parameter is specified)
        # For Dataset, data manipulation is performed
        # If result is a list or not a Dataset dict, no change - 7/13/2022 Max & Zhou
        return self.prepare_dataset(response.json())

    # The input `dataset_dict` can be a list if the entity-api returns a list, other times it's dict
    # Only applies to Dataset, no change to other entity types:
    # - Add the top-level 'files' field and set to empty list [] as default
    # - Set `ingest_metadata.files` to empty list [] when value is string (regardless empty or not) or the filed is missing
    # - Copy the actual files info list ['ingest_metadata']['files'] to the added top-level field
    # - Remove `ingest_metadata.metadata.*` sub fields when value is empty string
    def prepare_dataset(self, dataset_dict: dict):
        logger.info("Start executing prepare_dataset()")

        # Add this top-level field for Dataset and set to empty list as default
        if (isinstance(dataset_dict, dict)
                and "entity_type" in dataset_dict
                and dataset_dict["entity_type"] in ["Dataset", "Publication"]):

            dataset_dict["files"] = []

            if "ingest_metadata" in dataset_dict:
                if "files" in dataset_dict["ingest_metadata"]:
                    if isinstance(dataset_dict["ingest_metadata"]["files"], list):
                        # Copy the actual files info list to the added top-level field
                        dataset_dict["files"] = dataset_dict["ingest_metadata"]["files"]

                    elif isinstance(dataset_dict["ingest_metadata"]["files"], str):
                        # Set the original value to an emtpy list to avid mapping error
                        dataset_dict["ingest_metadata"]["files"] = []
                        logger.info(
                            "Set ['ingest_metadata']['files'] to empty list [] due "
                            f"to string value {dataset_dict['ingest_metadata']['files']} "
                            f"found, for Dataset {dataset_dict['uuid']}"
                        )

                    else:
                        logger.error(
                            "Invalid data type of ['ingest_metadata']['files'], "
                            f"for Dataset {dataset_dict['uuid']}"
                        )

                else:
                    dataset_dict["ingest_metadata"]["files"] = []
                    logger.info(
                        "Add missing field ['ingest_metadata']['files'] and set to "
                        f"empty list [], for Dataset {dataset_dict['uuid']}"
                    )

            # Remove any `ingest_metadata.metadata.*` sub fields if the value is empty string or just whitespace
            # to to avoid dynamic mapping conflict
            if ("ingest_metadata" in dataset_dict
                    and "metadata" in dataset_dict["ingest_metadata"]):

                for key in list(dataset_dict["ingest_metadata"]["metadata"]):
                    if isinstance(dataset_dict["ingest_metadata"]["metadata"][key], str):
                        if (not dataset_dict["ingest_metadata"]["metadata"][key]
                                or re.search(r"^\s+$", dataset_dict["ingest_metadata"]["metadata"][key])):

                            del dataset_dict["ingest_metadata"]["metadata"][key]
                            logger.info(
                                f"Removed ['ingest_metadata']['metadata']['{key}'] due to empty"
                                "string value, for Dataset {dataset_dict['uuid']}"
                            )

        logger.info("Finished executing prepare_dataset()")

        return dataset_dict

    def get_multi_revisions(self, entity_id: str, include_dataset: bool = None):
        url = self.entity_api_url + "/datasets/" + entity_id + "/multi-revisions"
        if include_dataset:
            url += "?include_dataset=true"

        if url in self.entity_api_cache:
            return copy.copy(self.entity_api_cache[url])

        response = requests.get(url, headers=self.request_headers, verify=False)
        if response.status_code != 200:
            msg = f"SenNet translator get_multi_revisions() failed to get entity of uuid {entity_id} via entity-api"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

            logger.debug("======get_multi_revisions() status code from entity-api======")
            logger.debug(response.status_code)

            logger.debug("======get_multi_revisions() response text from entity-api======")
            logger.debug(response.text)

            # Bubble up the error message from entity-api instead of sys.exit(msg)
            # The caller will need to handle this exception
            response.raise_for_status()
            raise requests.exceptions.RequestException(response.text)

        self.entity_api_cache[url] = response.json()

        return response.json()

    def get_collection_doc(self, entity_id: str):
        logger.info(f"Start executing get_collection_doc() on uuid: {entity_id}")

        # The entity-api returns public collection with a list of connected public/published datasets, for either
        # - a valid token but not in HuBMAP-Read group or
        # - no token at all
        # Here we do NOT send over the token
        url = self.entity_api_url + "/documents/" + entity_id
        response = requests.get(url, headers=self.request_headers, verify=False)

        if response.status_code != 200:
            msg = f"get_collection_doc() failed to get entity of uuid {entity_id} via entity-api"

            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

            logger.debug("======get_collection_doc() status code from entity-api======")
            logger.debug(response.status_code)

            logger.debug("======get_collection_doc() response text from entity-api======")
            logger.debug(response.text)

            # Bubble up the error message from entity-api instead of sys.exit(msg)
            # The caller will need to handle this exception
            response.raise_for_status()
            raise requests.exceptions.RequestException(response.text)

        collection_dict = response.json()

        logger.info(f"Finished executing get_collection_doc() on uuid: {entity_id}")

        return collection_dict

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

                    self.delete_index(public_index)
                    self.delete_index(private_index)

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

                    self.delete_index(public_index)
                    self.delete_index(private_index)

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

    def delete_index(self, index: str):
        try:
            self.indexer.delete_index(index)
        except Exception:
            pass

    def _get_existing_entity_relationships(self, entity_uuid: str, es_url: str, es_index: str):
        # Form a simple match query, and retrieve an existing OpenSearch document for entity_id, if it exists.
        # N.B. This query does not pass through the AWS Gateway, so we will not have to retrieve the
        # result from an AWS S3 Bucket. If it is larger than 10Mb, we will get it directly.
        QDSL_SEARCH_ENDPOINT_MATCH_UUID_PATTERN = (
            '{ '
            '"query": {  "bool": { "filter": [ {"terms": {"uuid": ["<TARGET_SEARCH_UUID>"]}} ] } }, '
            '"fields": ["ancestor_ids", "descendant_ids"] ,"_source": false'
            ' }'
        )

        qdsl_search_query_payload_string = (
            QDSL_SEARCH_ENDPOINT_MATCH_UUID_PATTERN.replace("<TARGET_SEARCH_UUID>", entity_uuid)
        )
        json_query_dict = json.loads(qdsl_search_query_payload_string)
        opensearch_response = execute_opensearch_query(
            query_against="_search",
            request=None,
            index=es_index,
            es_url=es_url,
            query=json_query_dict,
            request_params={"filter_path": "hits.hits"},
        )

        # Verify the expected response was returned.  If no document was returned, proceed with a re-indexing.
        # If exactly one document is returned, distill it down to JSON used to update document fields.
        if opensearch_response.status_code != 200:
            logger.error(
                "Unable to return ['hits']['hits'] content of opensearch_response for "
                f"es_url={es_url}, with "
                f"status_code={opensearch_response.status_code}."
            )
            raise Exception(
                f"OpenSearch query return a status code of '{opensearch_response.status_code}'. "
                "See logs."
            )

        resp_json = opensearch_response.json()

        if (not resp_json
                or "hits" not in resp_json
                or "hits" not in resp_json["hits"]
                or len(resp_json["hits"]["hits"]) == 0):
            # If OpenSearch does not have an existing document for this entity, drop down to reindexing.
            # Anything else Falsy JSON could be an unexpected result for an existing entity, but fall back to
            # reindexing under those circumstances, too.
            pass

        elif len(resp_json["hits"]["hits"]) != 1:
            # From the index populated with everything, raise an exception if exactly one document for the
            # current entity is not what is returned.
            logger.error(
                f"Found {len(resp_json['hits']['hits'])} documents instead "
                "of a single document searching resp_json['hits']['hits'] from opensearch_response with "
                f"es_url={es_url}, json_query_dict={json_query_dict}."
            )
            raise Exception(
                "Unexpected response to OpenSearch query for a single entity document. "
                "See logs."
            )

        elif "fields" not in resp_json["hits"]["hits"][0]:
            # The QDSL query may return exactly one resp_json['hits']['hits'] if called for an
            # entity which has a document but not the fields searched for e.g. a Source being
            # created with no ancestors or descendants yet. Return empty
            # JSON rather than indicating this is an error.
            return {}

        else:
            # Strip away whatever remains of OpenSearch artifacts, such as _source, to get to the
            # exact JSON of this entity's existing, so that can become a part of the other documents which
            # retain a snapshot of this entity, such as this entity's ancestors, this entity's descendants,
            # Collection entity's containing this entity, etc.
            # N.B. Many such artifacts should have already been stripped through usage of the filter_path.
            return resp_json["hits"]["hits"][0]

    def _directly_modify_related_entities(self, es_url: str, es_index: str, entity_id: str,
                                          neo4j_ancestor_ids: list[str], neo4j_descendant_ids: list[str],
                                          neo4j_collection_ids: list[str], neo4j_upload_ids: list[str]):
        # Directly update the OpenSearch documents for each associated entity with a current snapshot of
        # this entity, since relationships graph of this entity is unchanged in Neo4j since the
        # last time this entity was indexed.
        #
        # Given updated JSON for the OpenSearch document of this entity, replace the snapshot of
        # this entity's JSON in every OpenSearch document it appears in.
        # Each document for an identifier in the 'ancestors' list of this entity will need to have one
        # member of its 'descendants' list updated. Similarly, each OpenSearch document for a descendant
        # entity will need one member of its 'ancestors' list updated.
        # 'immediate_ancestors' will be updated for every entity which is an 'immediate_descendant'.
        # 'immediate_descendants' will be updated for every entity which is an 'immediate_ancestor'.

        # Retrieve the entity details
        # This returned entity dict (if Dataset) has removed ingest_metadata.files and
        # ingest_metadata.metadata sub fields with empty string values when call_entity_api() gets called
        revised_entity_doc_dict = self.call_entity_api(entity_id=entity_id, endpoint_base="documents")

        painless_query = (
            f'for (prop in <TARGET_DOC_ELEMENT_LIST>)'
            f' {{if (ctx._source.containsKey(prop))'
            f'  {{for (int i = 0; i < ctx._source[prop].length; ++i)'
            f'   {{if (ctx._source[prop][i][\'uuid\'] == params.modified_entity_uuid)'
            f'    {{ctx._source[prop][i] = params.revised_related_entity}} }} }} }}'
        )
        QDSL_UPDATE_ENDPOINT_WITH_ID_PARAM = (
            f'{{\"script\": {{'
            f'  \"lang\": \"painless\",'
            f'  \"source\": \"{painless_query}\",'
            f'  \"params\": {{'
            f'   \"modified_entity_uuid\": \"<TARGET_MODIFIED_ENTITY_UUID>\",'
            f'   \"revised_related_entity\": <THIS_REVISED_ENTITY>'
            f'  }}'
            f' }} }}'
        )

        # Eliminate taking advantage of our knowledge that an ancestor only needs its descendants lists
        # updated and a descendant only needs its ancestor lists updated.  Instead, focus upon consolidating
        # updates into a single query for the related entity's document to avoid HTTP 409 Conflict
        # problems if too many queries post for a single document.
        related_entity_target_elements = [
            "immediate_descendants",
            "descendants",
            "immediate_ancestors",
            "ancestors",
            "source_samples",
            "origin_samples",
            "datasets",
        ]

        related_entity_ids = (
            neo4j_ancestor_ids
            + neo4j_descendant_ids
            + neo4j_collection_ids
            + neo4j_upload_ids
        )

        for related_entity_id in related_entity_ids:
            qdsl_update_payload_string = (
                QDSL_UPDATE_ENDPOINT_WITH_ID_PARAM.replace("<TARGET_MODIFIED_ENTITY_UUID>", entity_id)
                .replace("<TARGET_DOC_ELEMENT_LIST>", str(related_entity_target_elements))
                .replace("<THIS_REVISED_ENTITY>", json.dumps(revised_entity_doc_dict))
            )
            json_query_dict = json.loads(qdsl_update_payload_string)

            opensearch_response = execute_opensearch_query(
                query_against=f"_update/{related_entity_id}",
                request=None,
                index=es_index,
                es_url=es_url,
                query=json_query_dict,
            )

            # Expect an HTTP 200 on a successful update, and an HTTP 404 if es_index does not
            # contain a document for related_entity_id.  Other response codes are errors.
            if opensearch_response.status_code not in [200, 404]:
                logger.error(
                    "Unable to directly update elements of document with "
                    f"related_entity_target_elements={related_entity_target_elements}, "
                    f"related_entity_id={related_entity_id}. "
                    f"Got status_code={opensearch_response.status_code} at "
                    f"es_url={es_url}, "
                    f"endoint '_update/{related_entity_id}' with "
                    f"qdsl_update_payload_string={qdsl_update_payload_string}."
                )
                raise Exception(
                    "OpenSearch query return a status code of "
                    f"'{opensearch_response.status_code}'. See logs."
                )

            elif opensearch_response.status_code == 404:
                logger.info(
                    "Call to QDSL _update got HTTP response code "
                    f"{opensearch_response.status_code}, which is ignored because it "
                    "should indicate "
                    f"related_entity_target_elements={related_entity_target_elements} "
                    f"is not in es_index={es_index}."
                )

    def _exec_reindex_entity_to_index_group_by_id(self, entity_id: str, index_group: str):
        logger.info(
            f"Start executing _exec_reindex_entity_to_index_group_by_id() on "
            f"entity_id: {entity_id}, index_group: {index_group}"
        )

        entity = self.call_entity_api(entity_id=entity_id, endpoint_base="documents")
        if entity["entity_type"] == "Collection":
            self.translate_collection(entity_id=entity_id, reindex=True)
        elif entity["entity_type"] == "Upload":
            self.translate_upload(entity_id=entity_id, reindex=True)
        else:
            self._transform_and_write_entity_to_index_group(entity=entity, index_group=index_group)

        logger.info("Finished executing _exec_reindex_entity_to_index_group_by_id()")

    def _transform_and_write_entity_to_index_group(self, entity: dict, index_group: str):
        logger.info(
            f"Start executing direct '{index_group}' updates for "
            f"entity['uuid']={entity['uuid']}, "
            f"entity['entity_type']={entity['entity_type']}"
        )
        try:
            private_doc = self.generate_doc(entity=entity, return_type="json")
            if self.is_public(entity):
                public_doc = self.generate_public_doc(entity=entity)

        except Exception:
            # Log the full stack trace, prepend a line with our message. But continue on
            # rather than raise the Exception.
            msg = (
                "Exception document generation "
                f"for uuid: {entity['uuid']}, entity_type: {entity['entity_type']}"
                f"for direct '{index_group}' reindex."
            )
            logger.exception(msg)

        docs_to_write_dict = {
            self.index_group_es_indices[index_group]["private"]: None,
            self.index_group_es_indices[index_group]["public"]: None,
        }
        # Check to see if the index_group has a transformer, default to None if not found
        transformer = self.TRANSFORMERS.get(index_group, None)
        if transformer is None:
            logger.info(f"Unable to find '{index_group}' transformer, indexing documents untransformed.")
            docs_to_write_dict[self.index_group_es_indices[index_group]["private"]] = private_doc
            if "public_doc" in locals() and public_doc is not None:
                docs_to_write_dict[self.index_group_es_indices[index_group]["public"]] = public_doc

        else:
            private_transformed = transformer.transform(json.loads(private_doc), self.transformation_resources)
            docs_to_write_dict[self.index_group_es_indices[index_group]["private"]] = json.dumps(private_transformed)

            if "public_doc" in locals() and public_doc is not None:
                public_transformed = transformer.transform(json.loads(public_doc), self.transformation_resources)
                docs_to_write_dict[self.index_group_es_indices[index_group]["public"]] = json.dumps(public_transformed)

        for index_name in docs_to_write_dict.keys():
            if docs_to_write_dict[index_name] is None:
                continue

            self.indexer.index(entity_id=entity["uuid"], document=docs_to_write_dict[index_name], index_name=index_name, reindex=True,)
            logger.info(
                f"Finished executing indexer.index() during direct '{index_group}' reindexing with "
                f"entity['uuid']={entity['uuid']}, "
                f"entity['entity_type']={entity['entity_type']}, "
                f"index_name={index_name}."
            )

        logger.info(
            f"Finished direct '{index_group}' updates for "
            f"entity['uuid']={entity['uuid']}, "
            f"entity['entity_type']={entity['entity_type']}"
        )

    def _relationships_changed_since_indexed(self, neo4j_ancestor_ids: list[str],
                                             neo4j_descendant_ids: list[str], existing_oss_doc: dict):
        # Start with the safe assumption that relationships have changed, and
        # only toggle if verified unchanged below
        relationships_changed = True

        # Get the ancestors and descendants of this entity as they exist in OpenSearch.
        oss_ancestor_ids = []
        if (existing_oss_doc
                and "fields" in existing_oss_doc
                and "ancestor_ids" in existing_oss_doc["fields"]):
            oss_ancestor_ids = existing_oss_doc["fields"]["ancestor_ids"]

        oss_descendant_ids = []
        if (existing_oss_doc
                and "fields" in existing_oss_doc
                and "descendant_ids" in existing_oss_doc["fields"]):
            oss_descendant_ids = existing_oss_doc["fields"]["descendant_ids"]

        # If the ancestor list and descendant list on the OpenSearch document for this entity are
        # not both exactly the same set of IDs as in Neo4j, relationships have changed and this
        # entity must be re-indexed rather than just updating existing documents for associated entities.
        #
        # These lists are implicitly sets, as they do not have duplicates and order does not mean anything.
        # Leave algorithmic efficiency to Python"s implementation of sets.
        neo4j_descendant_id_set = frozenset(neo4j_descendant_ids)
        oss_descendant_id_set = frozenset(oss_descendant_ids)

        if not neo4j_descendant_id_set.symmetric_difference(oss_descendant_id_set):
            # Since the descendants are unchanged, check the ancestors to decide if re-indexing must be done.
            neo4j_ancestor_id_set = frozenset(neo4j_ancestor_ids)
            oss_ancestor_id_set = frozenset(oss_ancestor_ids)

            if not neo4j_ancestor_id_set.symmetric_difference(oss_ancestor_id_set):
                relationships_changed = False

        return relationships_changed

    # Note: this entity dict input (if Dataset) has already removed ingest_metadata.files and
    # ingest_metadata.metadata sub fields with empty string values from previous call
    def _index_doc_directly_to_es_index(self, entity: dict, document: json, es_index: str,
                                        delete_existing_doc_first: bool = False):
        logger.info(
            "Start executing _index_doc_directly_to_es_index() on "
            f"uuid: {entity['uuid']}, entity_type: {entity['entity_type']}"
        )

        try:
            self.indexer.index(entity["uuid"], document, es_index, reindex=delete_existing_doc_first)
            logger.info(
                "Finished executing _index_doc_directly_to_es_index() on "
                f"uuid: {entity['uuid']}, entity_type: {entity['entity_type']}"
            )

        except Exception as e:
            msg = (
                f"Encountered exception e={str(e)}"
                " executing _index_doc_directly_to_es_index() with"
                f" uuid: {entity['uuid']}, entity_type: {entity['entity_type']}"
                f" es_index={es_index}"
            )
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def _reindex_related_entities(self, entity_id: str, entity_type: str, neo4j_ancestor_ids: list[str],
                                  neo4j_descendant_ids: list[str], neo4j_collection_ids: list[str],
                                  neo4j_upload_ids: list[str]):
        # If entity is new or Neo4j relationships for entity have changed, do a reindex with each ID
        # which has entity as an ancestor or descendant.  This is a costlier operation than
        # directly updating documents for related entities.
        previous_revision_ids = []
        next_revision_ids = []

        # Only Dataset/Publication entities may have previous/next revisions
        if entity_type in ["Dataset", "Publication"]:
            previous_revision_ids = self.call_entity_api(
                entity_id=entity_id,
                endpoint_base="previous_revisions",
                url_property="uuid"
            )
            next_revision_ids = self.call_entity_api(
                entity_id=entity_id,
                endpoint_base="next_revisions",
                url_property="uuid"
            )

        # All unique entity ids which might refer to the entity of entity_id
        target_ids = set(
            neo4j_ancestor_ids
            + neo4j_descendant_ids
            + previous_revision_ids
            + next_revision_ids
            + neo4j_collection_ids
            + neo4j_upload_ids
        )

        # Reindex the rest of the entities in the list
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures_list = [executor.submit(self.reindex_entity, uuid) for uuid in target_ids]
            for f in concurrent.futures.as_completed(futures_list):
                _ = f.result()

    # The added fields specified in `entity_properties_list` should not be added
    # to themselves as sub fields
    # The `except_properties_list` is a subset of entity_properties_list
    def exclude_added_top_level_properties(self, entity_data: Union[dict, list], except_properties_list: list = []):
        logger.info("Start executing exclude_added_top_level_properties()")

        if isinstance(entity_data, dict):
            for prop in entity_properties_list:
                if (prop in entity_data) and (prop not in except_properties_list):
                    entity_data.pop(prop)
        elif isinstance(entity_data, list):
            for prop in entity_properties_list:
                for item in entity_data:
                    if isinstance(item, dict) and (prop in item) and (prop not in except_properties_list):
                        item.pop(prop)
        else:
            logger.debug(f"The input entity_data type: {type(entity_data)}. Only dict and list are supported.")

        logger.info("Finished executing exclude_added_top_level_properties()")


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
