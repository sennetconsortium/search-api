import concurrent.futures
import copy
import json
import logging
import os
import sys
import time

from atlas_consortia_commons.string import equals
from atlas_consortia_commons.object import enum_val
from atlas_consortia_commons.ubkg import initialize_ubkg
from requests import HTTPError
from yaml import safe_load

from flask import Flask, Response

# HuBMAP commons
from hubmap_commons.hm_auth import AuthHelper
from libs.ontology import Ontology

sys.path.append("search-adaptor/src")
from indexer import Indexer
from opensearch_helper_functions import *
from translator.tranlation_helper_functions import *
from translator.translator_interface import TranslatorInterface
import datetime

logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

entity_properties_list = [
    'metadata',
    'source',
    'origin_sample',
    'source_sample',
    'ancestor_ids',
    'descendant_ids',
    'ancestors',
    'descendants',
    'files',
    'immediate_ancestors',
    'immediate_descendants',
    'datasets'
]

class Translator(TranslatorInterface):
    ACCESS_LEVEL_PUBLIC = 'public'
    ACCESS_LEVEL_CONSORTIUM = 'consortium'
    DATASET_STATUS_PUBLISHED = 'published'
    DEFAULT_INDEX_WITHOUT_PREFIX = ''
    INDICES = {}
    TRANSFORMERS = {}
    DEFAULT_ENTITY_API_URL = ''

    indexer = None
    entity_api_cache = {}

    def __init__(self, indices, app_client_id, app_client_secret, token, ubkg_instance=None):
        try:
            self.indices: dict = {}
            self.self_managed_indices: dict = {}
            # Do not include the indexes that are self managed
            for key, value in indices['indices'].items():
                if 'reindex_enabled' in value and value['reindex_enabled'] is True:
                    self.indices[key] = value
                else:
                    self.self_managed_indices[key] = value
            self.DEFAULT_INDEX_WITHOUT_PREFIX: str = indices['default_index']
            self.INDICES: dict = {'default_index': self.DEFAULT_INDEX_WITHOUT_PREFIX, 'indices': self.indices}
            self.DEFAULT_ENTITY_API_URL = self.INDICES['indices'][self.DEFAULT_INDEX_WITHOUT_PREFIX][
                'document_source_endpoint'].strip(
                '/')

            self.indexer = Indexer(self.indices, self.DEFAULT_INDEX_WITHOUT_PREFIX)
            self.ubkg_instance = ubkg_instance
            Ontology.set_instance(self.ubkg_instance)

            self.entity_types = Ontology.ops(as_arr=True, cb=enum_val).entities()
            self.entities = Ontology.ops().entities()

            logger.debug("@@@@@@@@@@@@@@@@@@@@ INDICES")
            logger.debug(self.INDICES)
        except Exception:
            raise ValueError("Invalid indices config")

        self.app_client_id = app_client_id
        self.app_client_secret = app_client_secret
        self.token = token

        self.auth_helper = self.init_auth_helper()
        self.request_headers = self.create_request_headers_for_auth(token)

        self.entity_api_url = self.indices[self.DEFAULT_INDEX_WITHOUT_PREFIX]['document_source_endpoint'].strip('/')

        # Add index_version by parsing the VERSION file
        self.index_version = ((Path(__file__).absolute().parent.parent / 'VERSION').read_text()).strip()

        with open(Path(__file__).resolve().parent / 'sennet_translation' / 'neo4j-to-es-attributes.json', 'r') as json_file:
            self.attr_map = json.load(json_file)

    def __get_scope_list(self, entity_id, document, index, scope):
        scope_list = []
        if index == 'files':
            # It would be nice if the possible scopes could be identified from
            # self.INDICES['indices'] rather than hardcoded. @TODO
            # This can handle indices besides "files" which might accept "scope" as
            # an argument, but returning an empty list, not raising an Exception, for
            # an  unrecognized index name.
            if scope is not None:
                if scope not in ['public', 'private']:
                    msg = (f"Unrecognized scope '{scope}' requested for"
                           f" entity_id '{entity_id}' in Dataset '{document['dataset_uuid']}.")
                    logger.info(msg)
                    raise ValueError(msg)
                elif scope == 'public':
                    if self.is_public(document):
                        scope_list.append(scope)
                    else:
                        # Reject the addition of 'public' was explicitly indicated, even though
                        # the public index may be silently skipped when a scope is not specified, in
                        # order to mimic behavior below for "non-self managed" indices.
                        msg = (f"Dataset '{document['dataset_uuid']}"
                               f" does not have status {self.DATASET_STATUS_PUBLISHED}, so"
                               f" entity_id '{entity_id}' cannot go in a public index.")
                        logger.info(msg)
                        raise ValueError(msg)
                elif scope == 'private':
                    scope_list.append(scope)
            else:
                scope_list = ['public', 'private']
        return scope_list

    def translate_all(self):
        with app.app_context():
            try:
                logger.info("############# Reindex Live Started #############")

                start = time.time()

                # Make calls to entity-api to get a list of uuids for each entity type
                source_uuids_list = get_uuids_by_entity_type("source", self.request_headers,
                                                             self.DEFAULT_ENTITY_API_URL)
                sample_uuids_list = get_uuids_by_entity_type("sample", self.request_headers,
                                                             self.DEFAULT_ENTITY_API_URL)
                dataset_uuids_list = get_uuids_by_entity_type("dataset", self.request_headers,
                                                              self.DEFAULT_ENTITY_API_URL)
                upload_uuids_list = get_uuids_by_entity_type("upload", self.request_headers,
                                                             self.DEFAULT_ENTITY_API_URL)

                collection_uuids_list = get_uuids_by_entity_type("collection", self.request_headers,
                                                                 self.DEFAULT_ENTITY_API_URL)

                logger.debug("merging sets into a one list...")
                # Merge into a big list that with no duplicates
                all_entities_uuids = set(
                    source_uuids_list + sample_uuids_list + dataset_uuids_list +
                    upload_uuids_list + collection_uuids_list
                )

                es_uuids = []
                # for index in ast.literal_eval(app.config['INDICES']).keys():
                logger.debug("looping through the indices...")
                logger.debug(self.INDICES['indices'].keys())

                index_names = get_all_reindex_enabled_indice_names(self.INDICES)
                logger.debug(self.INDICES['indices'].keys())

                for index in index_names.keys():
                    all_indices = index_names[index]
                    # get URL for that index
                    es_url = self.INDICES['indices'][index]['elasticsearch']['url'].strip('/')

                    for actual_index in all_indices:
                        es_uuids.extend(get_uuids_from_es(actual_index, es_url))

                es_uuids = set(es_uuids)

                logger.debug("looping through the UUIDs...")

                # Remove entities found in Elasticsearch but no longer in neo4j
                for uuid in es_uuids:
                    if uuid not in all_entities_uuids:
                        logger.debug(
                            f"Entity of uuid: {uuid} found in Elasticsearch but no longer in neo4j. Delete it from Elasticsearch.")
                        self.delete(uuid)

                logger.debug("Starting multi-thread reindexing ...")

                # Reindex in multi-treading mode for:
                # - each public collection
                # - each upload, only add to the hm_consortium_entities index (private index of the default)
                # - each source and its descendants in the tree
                futures_list = []
                results = []
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    collection_futures_list = [
                        executor.submit(self.translate_collection, uuid, reindex=True)
                        for uuid in collection_uuids_list]
                    upload_futures_list = [executor.submit(self.translate_upload, uuid, reindex=True) for uuid in upload_uuids_list]
                    source_futures_list = [executor.submit(self.translate_tree, uuid) for uuid in source_uuids_list]

                    # Append the above three lists into one
                    futures_list = collection_futures_list + upload_futures_list + source_futures_list

                    for f in concurrent.futures.as_completed(futures_list):
                        logger.debug(f.result())

                end = time.time()

                logger.info(
                    f"############# Live Reindex-All Completed. Total time used: {end - start} seconds. #############")
            except Exception as e:
                logger.error(e)

    def translate(self, entity_id):
        try:
            # Retrieve the entity details
            try:
                entity = self.call_entity_api(entity_id, 'entities')
            except HTTPError:
                entity = self.call_entity_api(entity_id, 'collections')

            # Check if entity is empty
            if bool(entity):
                logger.info(f"Executing translate() for entity_id: {entity_id}, entity_type: {entity['entity_type']}")

                if entity['entity_type'] == 'Collection':
                    # Expect entity-api to stop update of Collections which should not be modified e.g. those which
                    # have a DOI.  But entity-api may still request such Collections be indexed, particularly right
                    # after the Collection becomes visible to the public.
                    try:
                        self.translate_collection(entity_id
                                                  ,reindex=True)
                    except Exception as e:
                        logger.error(f"Unable to index Collection due to e={str(e)}")
                elif equals(entity['entity_type'], self.entities.UPLOAD):
                    self.translate_upload(entity_id, reindex=True)
                else:
                    previous_revision_entity_ids = []
                    next_revision_entity_ids = []

                    ancestor_entity_ids = self.call_entity_api(entity_id, 'ancestors', 'uuid')
                    descendant_entity_ids = self.call_entity_api(entity_id, 'descendants', 'uuid')

                    # Only Dataset entities may have previous/next revisions
                    if entity['entity_type'] in ['Dataset', 'Publication']:
                        previous_revision_entity_ids = self.call_entity_api(entity_id, 'previous_revisions',
                                                                            'uuid')
                        next_revision_entity_ids = self.call_entity_api(entity_id, 'next_revisions', 'uuid')

                    # Need to flatten previous and next revision lists
                    previous_revisions = [item for sublist in previous_revision_entity_ids for item in sublist]
                    next_revisions = [item for sublist in next_revision_entity_ids for item in sublist]
                    # All entity_ids in the path excluding the entity itself
                    entity_ids = ancestor_entity_ids + descendant_entity_ids + previous_revisions + next_revisions

                    self.call_indexer(entity)

                    # Reindex the rest of the entities in the list
                    for entity_entity_id in set(entity_ids):
                        # Retrieve the entity details
                        node = self.call_entity_api(entity_entity_id, 'entities')

                        self.call_indexer(node, True)

                logger.info("################reindex() DONE######################")

                # Clear the entity api cache
                self.entity_api_cache.clear()

                return "SenNet Translator.translate() finished executing"
        except Exception:
            msg = "Exceptions during executing indexer.reindex()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def update(self, entity_id, document, index=None, scope=None):
        if index is not None and index == 'files':
            # The "else clause" is the dominion of the original flavor of OpenSearch indices, for which search-api
            # was created.  This clause is specific to 'files' indices, by virtue of the conditions and the
            # following assumption that dataset_uuid is on the JSON body.
            scope_list = self.__get_scope_list(entity_id, document, index, scope)

            response = ''
            for scope in scope_list:
                target_index = self.self_managed_indices[index][scope]
                if scope == 'public' and not self.is_public(document):
                    # Mimic behavior of "else:" clause for "non-self managed" indices below, and
                    # silently skip public if it was put on the list by __get_scope_list() because
                    # the scope was not explicitly specified.
                    continue
                response += self.indexer.index(entity_id, json.dumps(document), target_index, True)
                response += '. '
        else:
            for index in self.indices.keys():
                public_index = self.INDICES['indices'][index]['public']
                private_index = self.INDICES['indices'][index]['private']

                if self.is_public(document):
                    response = self.indexer.index(entity_id, json.dumps(document), public_index, True)

                response += self.indexer.index(entity_id, json.dumps(document), private_index, True)
        return response

    def add(self, entity_id, document, index=None, scope=None):
        if index is not None and index == 'files':
            # The "else clause" is the dominion of the original flavor of OpenSearch indices, for which search-api
            # was created.  This clause is specific to 'files' indices, by virtue of the conditions and the
            # following assumption that dataset_uuid is on the JSON body.
            scope_list = self.__get_scope_list(entity_id, document, index, scope)

            response = ''
            for scope in scope_list:
                target_index = self.self_managed_indices[index][scope]
                if scope == 'public' and not self.is_public(document):
                    # Mimic behavior of "else:" clause for "non-self managed" indices below, and
                    # silently skip public if it was put on the list by __get_scope_list() because
                    # the scope was not explicitly specified.
                    continue
                response += self.indexer.index(entity_id, json.dumps(document), target_index, False)
                response += '. '
        else:
            public_index = self.INDICES['indices'][index]['public']
            private_index = self.INDICES['indices'][index]['private']

            if self.is_public(document):
                response =self.indexer.index(entity_id, json.dumps(document), public_index, False)
            response += self.indexer.index(entity_id, json.dumps(document), private_index, False)

        return response

    # This method is only applied to Collection/Donor/Sample/Dataset/File
    # Collection uses entity-api's logic for "visibility" to determine if a Collection is public or nonpublic
    # For Dataset, if status=='Published', it goes into the public index
    # For Source/Sample, `data`if any dataset down in the tree is 'Published', they should have `data_access_level` as public,
    # then they go into public index
    # Don't confuse with `data_access_level`
    def is_public(self, document):
        is_public = False

        if 'file_uuid' in document:
            # Confirm the Dataset to which the File entity belongs is published
            dataset = self.call_entity_api(document['dataset_uuid'], 'entities')
            return self.is_public(dataset)

        if document['entity_type'] in ['Dataset', 'Publication']:
            # In case 'status' not set
            if 'status' in document:
                if document['status'].lower() == self.DATASET_STATUS_PUBLISHED:
                    is_public = True
            else:
                # Log as an error to be fixed in Neo4j
                logger.error(
                    f"{document['entity_type']} of uuid: {document['uuid']} missing 'status' property, treat as not public, verify and set the status.")
        elif document['entity_type'] in ['Collection']:
            # If this Collection meets entity-api's criteria for visibility to the world by
            # returning the value of its schema_constants.py DataVisibilityEnum.PUBLIC,
            # the Collection can be in the public index and retrieved by users who are not logged in.
            entity_visibility = self.call_entity_api(document['uuid'], 'visibility')
            is_public = (entity_visibility == "public")
        else:
            # In case 'data_access_level' not set
            if 'data_access_level' in document:
                if document['data_access_level'].lower() == self.ACCESS_LEVEL_PUBLIC:
                    is_public = True
            else:
                # Log as an error to be fixed in Neo4j
                logger.error(
                    f"{document['entity_type']} of uuid: {document['uuid']} missing 'data_access_level' property, treat as not public, verify and set the data_access_level.")
        return is_public

    def delete_docs(self, index, scope, entity_id):
        # Clear multiple documents from the OpenSearch indices associated with the composite index specified
        # When index is for the files-api and entity_id is for a File, clear all file manifests for the File.
        # When index is for the files-api and entity_id is for a Dataset, clear all file manifests for the Dataset.
        # When index is for the files-api and entity_id is not specified, clear all file manifests in the index.
        # Otherwise, raise an Exception indicating the specified arguments are not supported.

        if not index:
            # Shouldn't happen due to configuration of Flask Blueprint routes
            raise ValueError(f"index must be specified for delete_docs()")

        if index == 'files':
            # For deleting documents, try removing them from the specified scope, but do not
            # raise any Exception or return an error response if they are not there to be deleted.
            scope_list = [scope] if scope else ['public', 'private']

            if entity_id:
                try:
                    # Get the Dataset entity with the specified entity_id
                    theEntity = self.call_entity_api(entity_id, 'entities')
                except Exception as e:
                    # entity-api may throw an Exception if entity_id is actually the
                    # uuid of a File, so swallow the error here and process as
                    # removing the file info document for a File below
                    logger.info(    f"No entity found  with entity_id '{entity_id}' in Neo4j, so process as"
                                    f" a request to delete a file info document for a File with that UUID.")
                    theEntity = {   'entity_type': 'File'
                                    ,'uuid': entity_id}

            response = ''
            for scope in scope_list:
                target_index = self.self_managed_indices[index][scope]
                if entity_id:
                    # Confirm the found entity for entity_id is of a supported type.  This probably repeats
                    # work done by the caller, but count on the caller for other business logic, like constraining
                    # to Datasets without PHI.
                    if theEntity and theEntity['entity_type'] not in ['Dataset',  'Publication', 'File']:
                        raise ValueError(   f"Translator.delete_docs() is not configured to clear documents for"
                                            f" entities of type '{theEntity['entity_type']} for HuBMAP.")
                    elif theEntity['entity_type'] in ['Dataset', 'Publication']:
                        try:
                            resp = self.indexer.delete_fieldmatch_document( target_index
                                                                            ,'dataset_uuid'
                                                                            , theEntity['uuid'])
                            response += resp[0]
                        except Exception as e:
                            response += (f"While deleting the Dataset '{theEntity['uuid']}' file info documents"
                                         f" from {target_index},"
                                         f" exception raised was {str(e)}.")
                    elif theEntity['entity_type'] == 'File':
                        try:
                            resp = self.indexer.delete_fieldmatch_document( target_index
                                                                            ,'file_uuid'
                                                                            ,theEntity['uuid'])
                            response += resp[0]
                        except Exception as e:
                            response += (   f"While deleting the File '{theEntity['uuid']}' file info document" 
                                            f" from {target_index},"
                                            f" exception raised was {str(e)}.")
                    else:
                        raise ValueError(   f"Unable to find a Dataset or File with identifier {entity_id} whose"
                                            f" file info documents can be deleted from OpenSearch.")
                else:
                    # Since a File or a Dataset was not specified, delete all documents from
                    # the target index.
                    response += self.indexer.delete_fieldmatch_document(target_index)
                response += ' '
            return response
        else:
            raise ValueError(f"The index '{index}' is not recognized for delete_docs() operations.")

    def delete(self, entity_id):
        for index, _ in self.indices.items():
            # each index should have a public/private index
            public_index = self.INDICES['indices'][index]['public']
            self.indexer.delete_document(entity_id, public_index)

            private_index = self.INDICES['indices'][index]['private']
            if public_index != private_index:
                self.indexer.delete_document(entity_id, private_index)

    # When indexing, Upload WILL NEVER BE PUBLIC
    def translate_upload(self, entity_id, reindex=False):
        try:
            logger.info(f"Start executing translate_upload() for {entity_id}")

            default_private_index = self.INDICES['indices'][self.DEFAULT_INDEX_WITHOUT_PREFIX]['private']

            # Retrieve the upload entity details
            upload = self.call_entity_api(entity_id, 'entities')

            self.add_entities_to_entity(upload)
            self.entity_keys_rename(upload)

            # Add additional calculated fields if any applies to Upload
            self.add_calculated_fields(upload)

            self.call_indexer(upload, reindex, json.dumps(upload), default_private_index)

            logger.info(f"Finished executing translate_upload() for {entity_id}")
        except Exception as e:
            logger.error(e)

    def translate_collection(self, entity_id, reindex=False):
        logger.info(f"Start executing translate_collection() for {entity_id}")

        # The entity-api returns public collection with a list of connected public/published datasets, for either
        # - a valid token but not in HuBMAP-Read group or
        # - no token at all
        # Here we do NOT send over the token
        try:
            collection = self.get_collection(entity_id=entity_id)

            self.add_entities_to_entity(collection)
            self.entity_keys_rename(collection)

            # Add additional calculated fields if any applies to Collection
            self.add_calculated_fields(collection)

            for index in self.indices.keys():
                # each index should have a public index
                public_index = self.INDICES['indices'][index]['public']
                private_index = self.INDICES['indices'][index]['private']

                # Add the tranformed doc to the portal index
                json_data = ""

                # if the index has a transformer use that else do a now load
                if self.TRANSFORMERS.get(index):
                    json_data = json.dumps(self.TRANSFORMERS[index].transform(collection))
                else:
                    json_data = json.dumps(collection)

                # If this Collection meets entity-api's criteria for visibility to the world by
                # returning the value of its schema_constants.py DataVisibilityEnum.PUBLIC, put
                # the Collection in the public index.
                if self.is_public(collection):
                    self.call_indexer(collection, reindex, json_data, public_index)
                self.call_indexer(collection, reindex, json_data, private_index)

            logger.info(f"Finished executing translate_collection() for {entity_id}")
        except requests.exceptions.RequestException as e:
            logger.exception(e)
            # Log the error and will need fix later and reindex, rather than sys.exit()
            logger.error(f"translate_collection() failed to get collection of uuid: {entity_id} via entity-api")
        except Exception as e:
            logger.error(e)

    def translate_tree(self, entity_id):
        try:
            # logger.info(f"Total threads count: {threading.active_count()}")

            logger.info(f"Executing index_tree() for source of uuid: {entity_id}")

            descendant_uuids = self.call_entity_api(entity_id, 'descendants', 'uuid')

            # Index the source entity itself separately
            source = self.call_entity_api(entity_id, 'entities')

            self.call_indexer(source)

            # Index all the descendants of this source
            for descendant_uuid in descendant_uuids:
                # Retrieve the entity details
                descendant = self.call_entity_api(descendant_uuid, 'entities')

                self.call_indexer(descendant)

            msg = f"indexer.index_tree() finished executing for source of uuid: {entity_id}"
            logger.info(msg)
            return msg
        except Exception as e:
            logger.error(e)

    #TODO: Delete later

    # def init_transformers(self):
    #     for index in self.indices.keys():
    #         try:
    #             xform_module = self.INDICES['indices'][index]['transform']['module']
    #
    #             logger.info(f"Transform module to be dynamically imported: {xform_module} at time: {time.time()}")
    #
    #             try:
    #                 m = importlib.import_module(xform_module)
    #                 self.TRANSFORMERS[index] = m
    #             except Exception as e:
    #                 logger.error(e)
    #                 msg = f"Failed to dynamically import transform module index: {index} at time: {time.time()}"
    #                 logger.exception(msg)
    #         except KeyError as e:
    #             logger.info(f'No transform or transform module specified in the search-config.yaml for index: {index}')
    #
    #     logger.debug("========Preloaded transformers===========")
    #     logger.debug(self.TRANSFORMERS)

    def init_auth_helper(self):
        if AuthHelper.isInitialized() == False:
            auth_helper = AuthHelper.create(self.app_client_id, self.app_client_secret)
        else:
            auth_helper = AuthHelper.instance()

        return auth_helper

    # Create a dict with HTTP Authorization header with Bearer token
    def create_request_headers_for_auth(self, token):
        auth_header_name = 'Authorization'
        auth_scheme = 'Bearer'

        headers_dict = {
            # Don't forget the space between scheme and the token value
            auth_header_name: auth_scheme + ' ' + token
        }

        return headers_dict

    def call_indexer(self, entity, reindex=False, document=None, target_index=None):
        org_node = copy.deepcopy(entity)

        try:
            if document is None:
                document = self.generate_doc(entity, 'json')

            if target_index:
                self.indexer.index(entity['uuid'], document, target_index, reindex)
            elif equals(entity['entity_type'], self.entities.UPLOAD):
                target_index = self.INDICES['indices'][self.DEFAULT_INDEX_WITHOUT_PREFIX]['private']
                self.indexer.index(entity['uuid'], document, target_index, reindex)
            else:
                # write entity into indices
                for index in self.indices.keys():
                    public_index = self.INDICES['indices'][index]['public']
                    private_index = self.INDICES['indices'][index]['private']

                    # check to see if the index has a transformer, default to None if not found
                    transformer = self.TRANSFORMERS.get(index, None)

                    if self.is_public(org_node):
                        public_doc = self.generate_public_doc(entity)

                        if transformer is not None:
                            public_transformed = transformer.transform(json.loads(public_doc))
                            public_transformed_doc = json.dumps(public_transformed)
                            target_doc = public_transformed_doc
                        else:
                            target_doc = public_doc

                        self.indexer.index(entity['uuid'], target_doc, public_index, reindex)

                    # add it to private
                    if transformer is not None:
                        private_transformed = transformer.transform(json.loads(document))
                        target_doc = json.dumps(private_transformed)
                    else:
                        target_doc = document

                    self.indexer.index(entity['uuid'], target_doc, private_index, reindex)
        except Exception:
            msg = f"Exception encountered during executing Translator call_indexer() for uuid: {org_node['uuid']}, entity_type: {org_node['entity_type']}"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    # Used for Upload and Collection index
    def add_entities_to_entity(self, entity):
        entities = []

        def handle_doc(key):
            for _entity in entity[key]:
                # Retrieve the entity details
                _entity = self.call_entity_api(_entity['uuid'], 'entities')

                entity_doc = self.generate_doc(_entity, 'dict')
                entity_doc.pop('ancestors')
                entity_doc.pop('ancestor_ids')
                entity_doc.pop('descendants')
                entity_doc.pop('descendant_ids')
                entity_doc.pop('immediate_descendants')
                entity_doc.pop('immediate_ancestors')
                entity_doc.pop('source')
                # entity_doc.pop('origin_sample')
                if 'source_sample' in entity_doc:
                    entity_doc.pop('source_sample')

                entities.append(entity_doc)

            entity[key] = entities

        if 'datasets' in entity:
            handle_doc('datasets')

        if 'entities' in entity:
            handle_doc('entities')

    def entity_keys_rename(self, entity):
        logger.info("Start executing entity_keys_rename()")

        # logger.debug("==================entity before renaming keys==================")
        # logger.debug(entity)

        to_delete_keys = []
        temp = {}

        for key in entity:
            to_delete_keys.append(key)
            if key in self.attr_map['ENTITY']:
                # Special case of Sample.rui_location
                # To be backward compatible for API clients relying on the old version
                # Also gives the ES consumer flexibility to change the inner structure
                # Note: when `rui_location` is stored as json object (Python dict) in ES
                # with the default dynamic mapping, it can cause errors due to
                # the changing data types of some internal fields
                # isinstance() check is to avoid json.dumps() on json string again
                if (key == 'rui_location') and isinstance(entity[key], dict):
                    # Convert Python dict to json string
                    temp_val = json.dumps(entity[key])
                else:
                    temp_val = entity[key]

                temp[self.attr_map['ENTITY'][key]['es_name']] = temp_val

        for key in to_delete_keys:
            if key not in entity_properties_list:
                entity.pop(key)

        entity.update(temp)

        # logger.debug("==================entity after renaming keys==================")
        # logger.debug(entity)

        logger.info("Finished executing entity_keys_rename()")


    # These calculated fields are not stored in neo4j but will be generated
    # and added to the ES
    def add_calculated_fields(self, entity):
        # Add index_version by parsing the VERSION file
        entity['index_version'] = self.index_version

        # Add display_subtype
        if entity['entity_type'] in self.entity_types:
            entity['display_subtype'] = self.generate_display_subtype(entity)

        # Add has rui information for all non-organ samples and datasets
        if (entity['entity_type'] in ['Sample', 'Dataset']
                and entity.get('sample_category') != 'Organ'
                and entity.get('source', {}).get('source_type') in ['Human', 'Human Organoid']):
            ancestors = entity.get('ancestors', [])
            organs = set([a['organ'] for a in ancestors if 'organ' in a])
            if len(organs.intersection({'AD', 'BD', 'BM', 'BS', 'MU', 'OT'})) > 0:
                # Has an organ ancestor that is not supported
                entity['has_rui_information'] = 'N/A'
            else:
                has_rui = ('rui_location' in entity or
                           len([a for a in ancestors if 'rui_location' in a]) > 0)
                entity['has_rui_information'] = str(has_rui)

        last_touch = entity['published_timestamp'] if 'published_timestamp' in entity else entity['last_modified_timestamp']
        entity['last_touch'] = str(datetime.datetime.utcfromtimestamp(last_touch/1000))

        # Add dataset category
        if entity['entity_type'] == 'Dataset' and 'creation_action' in entity:
            dataset_category_map = {
                'Create Dataset Activity': 'primary',
                'Multi-Assay Split': 'component',
                'Central Process': 'codcc-processed',
                'Lab Process': 'lab-processed',
            }
            if dataset_category := dataset_category_map.get(entity['creation_action']):
                entity['dataset_category'] = dataset_category


    # For Upload, Dataset, Source and Sample objects:
    # add a calculated (not stored in Neo4j) field called `display_subtype` to
    # all Elasticsearch documents of the above types with the following rules:
    # Upload: Just make it "Data Upload" for all uploads
    # Source: "Source"
    # Sample: if sample_category == 'organ' the display name linked to the corresponding description of organ code
    # otherwise the display name linked to the value of the corresponding description of sample_category code
    def generate_display_subtype(self, entity):
        entity_type = entity['entity_type']
        display_subtype = '{unknown}'

        if equals(entity_type, self.entities.SOURCE):
            display_subtype = entity['source_type']
        elif equals(entity_type, self.entities.SAMPLE):
            if 'sample_category' in entity:
                if equals(entity['sample_category'], Ontology.ops().specimen_categories().ORGAN):
                    if 'organ' in entity:
                        organ_types = Ontology.ops(as_data_dict=True, prop_callback=None, key='rui_code', val_key='term').organ_types()
                        organ_types['OT'] = 'Other'
                        display_subtype = get_val_by_key(entity['organ'], organ_types, 'ubkg.organ_types')
                    else:
                        logger.error(
                            f"Missing missing organ when sample_category is set of Sample with uuid: {entity['uuid']}")
                else:
                    sample_categories = Ontology.ops(as_data_dict=True, prop_callback=None).specimen_categories()
                    display_subtype = get_val_by_key(entity['sample_category'], sample_categories, 'ubkg.specimen_categories')
            else:
                logger.error(f"Missing sample_category of Sample with uuid: {entity['uuid']}")
        elif equals(entity_type, self.entities.DATASET):
            if 'dataset_type' in entity:
                display_subtype = entity['dataset_type']
            else:
                logger.error(f"Missing dataset_type of Dataset with uuid: {entity['uuid']}")
        elif equals(entity_type, self.entities.UPLOAD):
            display_subtype = 'Data Upload'
        else:
            # Do nothing
            logger.error(
                f"Invalid entity_type: {entity_type}. Only generate display_subtype for Source/Sample/Dataset/Upload")

        return display_subtype

    def generate_doc(self, entity, return_type):
        try:
            entity_id = entity['uuid']

            if not equals(entity['entity_type'], self.entities.UPLOAD):
                ancestors = []
                descendants = []
                ancestor_ids = []
                descendant_ids = []
                immediate_ancestors = []
                immediate_descendants = []

                # Do not call /ancestors/<id> directly to avoid performance/timeout issue
                ancestor_ids = self.call_entity_api(entity_id, 'ancestors', 'uuid')

                for ancestor_uuid in ancestor_ids:
                    # Retrieve the entity details
                    ancestor_dict = self.call_entity_api(ancestor_uuid, 'entities')

                    # Add to the list
                    ancestors.append(ancestor_dict)

                # Find the Source
                source = None
                for a in ancestors:
                    if equals(a['entity_type'], self.entities.SOURCE):
                        source = copy.copy(a)
                        break

                descendant_ids = self.call_entity_api(entity_id, 'descendants', 'uuid')

                for descendant_uuid in descendant_ids:
                    # Retrieve the entity details
                    descendant_dict = self.call_entity_api(descendant_uuid, 'entities')

                    # Add to the list
                    descendants.append(descendant_dict)

                # Calls to /parents/<id> and /children/<id> have no performance/timeout concerns
                immediate_ancestors = self.call_entity_api(entity_id, 'parents')
                immediate_descendants = self.call_entity_api(entity_id, 'children')

                # Add new properties to entity
                entity['ancestors'] = ancestors
                entity['descendants'] = descendants

                entity['ancestor_ids'] = ancestor_ids
                entity['descendant_ids'] = descendant_ids

                entity['immediate_ancestors'] = immediate_ancestors
                entity['immediate_descendants'] = immediate_descendants

            # The origin_sample is the sample that `sample_category` is "organ" and the `organ` code is set at the same time
            if entity['entity_type'] in ['Sample', 'Dataset', 'Publication']:
                # Add new properties
                entity['source'] = source

                sample_categories = Ontology.ops().specimen_categories()

                entity['origin_sample'] = None 
                if ('sample_category' in entity and
                    'organ' in entity and
                    entity['organ'].strip() != '' and
                    equals(entity['sample_category'], sample_categories.ORGAN)):
                    entity['origin_sample'] = copy.copy(entity) 

                if entity['origin_sample'] is None:
                    try:
                        # The origin_sample is the ancestor which `sample_category` is "organ" and the `organ` code is set
                        itr = next(a for a in ancestors
                                   if ('sample_category' in a and
                                       'organ' in a and
                                       a['organ'].strip() != '' and
                                       equals(a['sample_category'], sample_categories.ORGAN)))
                        entity['origin_sample'] = copy.copy(itr)
                    except StopIteration:
                        entity['origin_sample'] = {}

                # Trying to understand here!!!
                if entity['entity_type'] in ['Dataset', 'Publication']:
                    entity['source_sample'] = None

                    e = entity

                    while entity['source_sample'] is None:
                        parents = self.call_entity_api(e['uuid'], 'parents')

                        try:
                            # Why?
                            if equals(parents[0]['entity_type'], self.entities.SAMPLE):
                                entity['source_sample'] = parents

                            e = parents[0]
                        except IndexError:
                            entity['source_sample'] = {}

                    # Move files to the root level if exist
                    if 'ingest_metadata' in entity and equals(entity['entity_type'], self.entities.DATASET):
                        # Because we remove files from metadata later (to reduce size) we need to shallow copy of metadata
                        metadata = copy.copy(entity['ingest_metadata'])
                        if 'files' in metadata:
                            entity['files'] = metadata['files']
                            entity['ingest_metadata'].pop('files')

                    # Add multi-revisions
                    if 'next_revision_uuid' in entity or 'previous_revision_uuid' in entity:
                        multi_revisions = self.get_multi_revisions(entity_id, True)
                        if multi_revisions:
                            entity['multi_revisions'] = multi_revisions

            self.entity_keys_rename(entity)


            if entity.get('origin_sample', None):
                self.entity_keys_rename(entity['origin_sample'])
            if entity.get('source_sample', None):
                for s in entity.get('source_sample', None):
                    self.entity_keys_rename(s)
            if entity.get('ancestors', None):
                for a in entity.get('ancestors', None):
                    self.entity_keys_rename(a)
            if entity.get('descendants', None):
                for d in entity.get('descendants', None):
                    self.entity_keys_rename(d)
            if entity.get('immediate_descendants', None):
                for parent in entity.get('immediate_descendants', None):
                    self.entity_keys_rename(parent)
            if entity.get('immediate_ancestors', None):
                for child in entity.get('immediate_ancestors', None):
                    self.entity_keys_rename(child)

            remove_specific_key_entry(entity, "other_metadata")

            # Add additional calculated fields
            self.add_calculated_fields(entity)

            return json.dumps(entity) if return_type == 'json' else entity
        except Exception:
            msg = "Exceptions during executing indexer.generate_doc()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def generate_public_doc(self, entity):
        # Only Dataset has this 'next_revision_uuid' property
        property_key = 'next_revision_uuid'
        if (entity['entity_type'] in ['Dataset', 'Publication']) and (property_key in entity):
            next_revision_uuid = entity[property_key]
            # Making a call against entity-api/entities/<next_revision_uuid>?property=status
            url = self.entity_api_url + "/entities/" + next_revision_uuid + "?property=status"
            response = requests.get(url, headers=self.request_headers, verify=False)

            if response.status_code != 200:
                msg = f"indexer.generate_public_doc() failed to get status of next_revision_uuid via entity-api for uuid: {next_revision_uuid}"
                logger.error(msg)
                sys.exit(msg)

            # The call to entity-api returns string directly
            dataset_status = (response.text).lower()

            # Check the `next_revision_uuid` and if the dataset is not published,
            # pop the `next_revision_uuid` from this entity
            if dataset_status != self.DATASET_STATUS_PUBLISHED:
                logger.debug(f"Remove the {property_key} property from {entity['uuid']}")
                entity.pop(property_key)

        entity['descendants'] = list(filter(self.is_public, entity['descendants']))
        entity['immediate_descendants'] = list(filter(self.is_public, entity['immediate_descendants']))
        return json.dumps(entity)

    def call_entity_api(self, entity_id, endpoint, url_property=None):
        url = self.entity_api_url + "/" + endpoint + "/" + entity_id
        if url_property:
            url += "?property=" + url_property

        if url in self.entity_api_cache:
            return copy.copy(self.entity_api_cache[url])

        response = requests.get(url, headers=self.request_headers, verify=False)
        if response.status_code != 200:
            msg = f"SenNet translator call_entity_api() failed to get entity of uuid {entity_id} via entity-api"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

            logger.debug("======call_entity_api() status code from entity-api======")
            logger.debug(response.status_code)

            logger.debug("======call_entity_api() response text from entity-api======")
            logger.debug(response.text)

            # Bubble up the error message from entity-api instead of sys.exit(msg)
            # The caller will need to handle this exception
            response.raise_for_status()
            raise requests.exceptions.RequestException(response.text)

        self.entity_api_cache[url] = response.json()

        return response.json()

    def get_multi_revisions(self, entity_id, include_dataset=None):
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

    def get_collection(self, entity_id):
        logger.info(f"Start executing get_collection() on uuid: {entity_id}")
        # The entity-api returns public collection with a list of connected public/published datasets, for either
        # - a valid token but not in Globus Read group or
        # - no token at all
        # Here we do NOT send over the token
        url = self.entity_api_url + "/entities/" + entity_id
        response = requests.get(url, headers=self.request_headers, verify=False)

        if response.status_code != 200:
            msg = f"get_collection() failed to get entity of uuid {entity_id} via entity-api"

            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

            logger.debug("======get_collection() status code from sennet_translator======")
            logger.debug(response.status_code)

            logger.debug("======get_collection() response text from sennet_translator-api======")
            logger.debug(response.text)

            # Bubble up the error message from entity-api instead of sys.exit(msg)
            # The caller will need to handle this exception
            response.raise_for_status()
            raise requests.exceptions.RequestException(response.text)

        collection_dict = response.json()
        logger.info(f"Finished executing get_collection() on uuid: {entity_id}")

        return collection_dict

    def delete_and_recreate_indices(self, files=False):
        try:
            logger.info("Start executing delete_and_recreate_indices()")
            public_index = None
            private_index = None

            # Delete and recreate target indices
            if files:
                for index in self.self_managed_indices.keys():
                    public_index = self.self_managed_indices[index]['public']
                    private_index = self.self_managed_indices[index]['private']

                    self.delete_index(public_index)
                    self.delete_index(private_index)

                    index_mapping_file = self.self_managed_indices[index]['elasticsearch']['mappings']

                    # read the elasticserach specific mappings
                    index_mapping_settings = safe_load(
                        (Path(__file__).absolute().parent / index_mapping_file).read_text())

                    self.indexer.create_index(public_index, index_mapping_settings)
                    self.indexer.create_index(private_index, index_mapping_settings)

                    logger.info("Finished executing delete_and_recreate_indices()")


            else:
                for index in self.indices.keys():
                    # each index should have a public/private index
                    public_index = self.INDICES['indices'][index]['public']
                    private_index = self.INDICES['indices'][index]['private']

                    self.delete_index(public_index)
                    self.delete_index(private_index)

                    # get the specific mapping file for the designated index
                    index_mapping_file = self.INDICES['indices'][index]['elasticsearch']['mappings']

                    # read the elasticserach specific mappings
                    index_mapping_settings = safe_load((Path(__file__).absolute().parent / index_mapping_file).read_text())

                    self.indexer.create_index(public_index, index_mapping_settings)
                    self.indexer.create_index(private_index, index_mapping_settings)

                    logger.info("Finished executing delete_and_recreate_indices()")
        except Exception:
            msg = "Exception encountered during executing delete_and_recreate_indices()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)

    def delete_index(self, index):
        try:
            self.indexer.delete_index(index)
        except Exception as e:
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
    app = Flask(__name__, instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__)), '../src/instance'),
                instance_relative_config=True)
    app.config.from_pyfile('app.cfg')
    ubkg_instance = initialize_ubkg(app.config)

    INDICES = safe_load((Path(__file__).absolute().parent / 'instance/search-config.yaml').read_text())

    try:
        token = sys.argv[1]
    except IndexError as e:
        msg = "Missing admin group token argument"
        logger.exception(msg)
        sys.exit(msg)

    # Create an instance of the indexer
    translator = Translator(INDICES, app.config['APP_CLIENT_ID'], app.config['APP_CLIENT_SECRET'], token, ubkg_instance)

    auth_helper = translator.init_auth_helper()

    # The second argument indicates to get the groups information
    user_info_dict = auth_helper.getUserInfo(token, True)

    if isinstance(user_info_dict, Response):
        msg = "The given token is expired or invalid"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        sys.exit(msg)

    # Use the new key rather than the 'hmgroupids' which will be deprecated
    group_ids = user_info_dict['group_membership_ids']

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