import json
import logging
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from typing import Any

from atlas_consortia_commons.rest import rest_not_found, rest_server_err
from flask import Blueprint, current_app, g, request
from mysql.connector.pooling import MySQLConnectionPool
from pymongo.collection import Collection
from requests import Session

from libs.elasticsearch import ESBulkUpdater, get_docs_from_es
from libs.hash import calculate_sha256_hash
from libs.http import new_session
from libs.ontology import GeneManager

logger = logging.getLogger()


senotypes_blueprint = Blueprint("senotypes", __name__)

# Keep this executor alive for the process lifetime so request handlers do not
# block waiting for background tasks to finish.
background_executor = ThreadPoolExecutor(max_workers=2)


# Auth handled in gateway
@senotypes_blueprint.route("/senotypes/reindex/test/<string:id>", methods=["PUT"])
def reindex_test_senotype(id: str):
    # check that the senotypes index configuration exists before proceeding
    search_config = current_app.config["SEARCH_CONFIG"]
    senotypes_config = search_config.get("indices", {}).get("senotypes-test")
    if not senotypes_config:
        return rest_server_err("Senotypes test index configuration not found")

    try:
        collection = current_app.config["MONGO_DB"]["senotypes"]
        doc = collection.find_one({"uuid": id}, {"_id": 0})
        if not doc:
            return rest_not_found(f"Senotype with uuid {id} not found")
    except Exception as e:
        logger.exception(f"Failed to retrieve senotype with uuid {id} from MongoDB: {e}")
        return rest_server_err(f"Failed to retrieve senotype with uuid {id}")

    try:
        es_url = senotypes_config["elasticsearch"]["url"]
        priv_index = senotypes_config["private"]
        with ESBulkUpdater(es_url=es_url, index=priv_index) as priv_updater:
            actual_sha256 = calculate_sha256_hash(doc)
            doc["doc_sha256"] = actual_sha256
            priv_updater.add_delete(doc_id=doc["uuid"])
            priv_updater.add_update(doc_id=doc["uuid"], doc=doc, upsert=True)
    except Exception as e:
        logger.exception(f"Failed to index senotype with uuid {id} into Elasticsearch: {e}")
        return rest_server_err(f"Failed to index senotype with uuid {id} into Elasticsearch")

    return {"message": f"Senotype with uuid {doc["uuid"]} reindexed successfully"}, 200


# Auth handled in gateway
@senotypes_blueprint.route("/senotypes/reindex-all/test", methods=["PUT"])
def reindex_all_test_senotypes():
    # check that the senotypes index configuration exists before proceeding
    search_config = current_app.config["SEARCH_CONFIG"]
    senotypes_config = search_config.get("indices", {}).get("senotypes-test")
    if not senotypes_config:
        return rest_server_err("Senotypes test index configuration not found")

    token = request.authorization.token if request.authorization else None
    if not token:
        return rest_server_err("Authorization token is required")

    future = background_executor.submit(
        _reindex_test_senotypes_thread,
        senotypes_config=senotypes_config,
        collection=current_app.config["MONGO_DB"]["senotypes"],
    )
    future.add_done_callback(_log_reindex_all_result)

    return {"message": "Request of reindex all senotypes accepted"}, 202


def _reindex_test_senotypes_thread(senotypes_config: dict, collection: Collection):
    try:
        items = [item for item in collection.find({}, {"_id": 0})]
    except Exception as e:
        logger.exception(f"Failed to retrieve senotypes from MongoDB: {e}")
        return

    es_url = senotypes_config["elasticsearch"]["url"]
    priv_index = senotypes_config["private"]

    with new_session(es_url) as session:
        try:
            uuids = [item["uuid"] for item in items]
            private_sha256s = {
                doc["_id"]: doc["doc_sha256"]
                for doc in get_docs_from_es(
                    index=priv_index,
                    es_url=senotypes_config["elasticsearch"]["url"],
                    fields=["doc_sha256"],
                    session=session,
                    query={"terms": {"_id": uuids}},
                )
                if "doc_sha256" in doc
            }
        except Exception as e:
            logger.exception(f"Failed to retrieve existing senotypes data from Elasticsearch: {e}")
            return

        try:
            with ESBulkUpdater(es_url=es_url, index=priv_index, session=session) as priv_updater:
                # delete any senotypes from ES that no longer exist in the database
                docs_to_delete = set(private_sha256s.keys()) - set(uuids)
                for doc_id in docs_to_delete:
                    priv_updater.add_delete(doc_id=doc_id)

                # add or update senotypes in ES based on the database records
                for item in items:
                    try:
                        actual_sha256 = calculate_sha256_hash(item)
                        if private_sha256s.get(item["uuid"]) is None:
                            # doc doesn't exist in ES, create it
                            item["doc_sha256"] = actual_sha256
                            priv_updater.add_update(doc_id=item["uuid"], doc=item, upsert=True)
                        elif private_sha256s[item["uuid"]] != actual_sha256:
                            # doc exists but has changed, update it in ES
                            item["doc_sha256"] = actual_sha256
                            priv_updater.add_delete(doc_id=item["uuid"])
                            priv_updater.add_update(doc_id=item["uuid"], doc=item, upsert=True)

                    except Exception as e:
                        logger.exception(f"Failed to index senotype with uuid {item['uuid']}: {e}")

        except Exception as e:
            logger.exception(f"Failed to index senotypes into Elasticsearch: {e}")
            return

    logger.info("Finished reindexing senotypes")


# Auth handled in gateway
@senotypes_blueprint.route("/senotypes/reindex/<string:id>", methods=["PUT"])
def reindex_senotype(id: str):
    # check that the senotypes index configuration exists before proceeding
    search_config = current_app.config["SEARCH_CONFIG"]
    senotypes_config = search_config.get("indices", {}).get("senotypes")
    if not senotypes_config:
        return rest_server_err("Senotypes index configuration not found")

    # retrieve the senotype record from the database
    with g.db.cursor(dictionary=True) as cursor:
        cursor.execute("SELECT senotypeid, senotypejson FROM senotype WHERE senotypeid = %s", (id,))
        row = cursor.fetchone()

    if row is None:
        return rest_not_found(f"Senotype with id {id} not found")

    senotypeid = row["senotypeid"]
    senotypejson = row["senotypejson"]
    if not senotypeid or not senotypejson or not isinstance(senotypejson, str):
        logger.warning(f"Invalid data for senotype with id {senotypeid}: {row}")
        return rest_server_err(f"Invalid data for senotype with id {senotypeid}")

    token = request.authorization.token if request.authorization else None
    if not token:
        return rest_server_err("Authorization token is required")

    entity_api_url = current_app.config["ENTITY_API_URL"]
    es_url = senotypes_config["elasticsearch"]["url"]
    ubkg_url = current_app.config["UBKG_SERVER"].rstrip("/")

    with new_session([entity_api_url, es_url]) as session, GeneManager(ubkg_url) as gene_manager:
        try:
            doc = _build_doc(
                id=senotypeid,
                row=senotypejson,
                session=session,
                entity_api_url=entity_api_url,
                token=token,
                gene_manager=gene_manager,
            )

        except Exception as e:
            logger.exception(f"Failed to build doc for senotype {senotypeid}: {e}")
            return rest_server_err(f"Failed to build doc for senotype {senotypeid}")

        with ESBulkUpdater(
            es_url=senotypes_config["elasticsearch"]["url"],
            index=senotypes_config["private"],
            session=session,
        ) as priv_updater:
            actual_sha256 = calculate_sha256_hash(doc)
            doc["doc_sha256"] = actual_sha256
            priv_updater.add_delete(doc_id=senotypeid)
            priv_updater.add_update(doc_id=senotypeid, doc=doc, upsert=True)

    return {"message": f"Senotype with id {senotypeid} reindexed successfully"}, 200


# Auth handled in gateway
@senotypes_blueprint.route("/senotypes/reindex-all", methods=["PUT"])
def reindex_all_senotypes():
    # check that the senotypes index configuration exists before proceeding
    search_config = current_app.config["SEARCH_CONFIG"]
    senotypes_config = search_config.get("indices", {}).get("senotypes")
    if not senotypes_config:
        return rest_server_err("Senotypes index configuration not found")

    token = request.authorization.token if request.authorization else None
    if not token:
        return rest_server_err("Authorization token is required")

    future = background_executor.submit(
        _reindex_senotypes_thread,
        senotypes_config=senotypes_config,
        entity_api_url=current_app.config["ENTITY_API_URL"].rstrip("/"),
        ubkg_url=current_app.config["UBKG_SERVER"].rstrip("/"),
        token=token,
        db_pool=current_app.config["DB_POOL"],
    )
    future.add_done_callback(_log_reindex_all_result)

    return {"message": "Request of reindex all senotypes accepted"}, 202


@senotypes_blueprint.route("/senotypes/cache", methods=["DELETE"])
def delete_senotype_cache():
    try:
        with GeneManager(current_app.config["UBKG_SERVER"].rstrip("/")) as gene_manager:
            gene_manager.clear_cache()
        return {"message": "Gene cache cleared successfully"}, 200
    except Exception as e:
        logger.exception(f"Failed to clear gene cache: {e}")
        return rest_server_err("Failed to clear gene cache")


@senotypes_blueprint.before_request
def open_db():
    """Open a new database connection before each request for this blueprint."""
    g.db = current_app.config["DB_POOL"].get_connection()


@senotypes_blueprint.teardown_request
def close_db(error):
    """Return the connection to the pool after each request for this blueprint."""
    db = g.pop("db", None)
    if db is not None:
        db.close()


def _reindex_senotypes_thread(
    senotypes_config: dict,
    entity_api_url: str,
    ubkg_url: str,
    token: str,
    db_pool: MySQLConnectionPool,
):
    try:
        # retrieve all senotype records from the database
        connection = db_pool.get_connection()
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute("SELECT senotypeid, senotypejson FROM senotype")
            rows = cursor.fetchall()
    except Exception as e:
        logger.exception(f"Failed to retrieve senotypes from database: {e}")
        return
    finally:
        if "connection" in locals() and connection is not None:
            connection.close()

    es_url = senotypes_config["elasticsearch"]["url"]

    with new_session([entity_api_url, es_url]) as session, GeneManager(ubkg_url) as gene_manager:
        senotypeids = [row["senotypeid"] for row in rows]
        try:
            private_sha256s = {
                doc["_id"]: doc["doc_sha256"]
                for doc in get_docs_from_es(
                    index=senotypes_config["private"],
                    es_url=senotypes_config["elasticsearch"]["url"],
                    fields=["doc_sha256"],
                    session=session,
                    query={"terms": {"_id": senotypeids}},
                )
                if "doc_sha256" in doc
            }
        except Exception as e:
            logger.exception(f"Failed to retrieve existing senotypes from Elasticsearch: {e}")
            return

        with ESBulkUpdater(
            es_url=senotypes_config["elasticsearch"]["url"],
            index=senotypes_config["private"],
            session=session,
        ) as priv_updater:
            # delete any senotypes from ES that no longer exist in the database
            docs_to_delete = set(private_sha256s.keys()) - set(senotypeids)
            for doc_id in docs_to_delete:
                priv_updater.add_delete(doc_id=doc_id)

            # add or update senotypes in ES based on the database records
            for row in rows:
                try:
                    senotypeid = row["senotypeid"]
                    senotypejson = row["senotypejson"]
                    if not senotypeid or not senotypejson or not isinstance(senotypejson, str):
                        logger.warning(f"Skipping row with missing id or payload: {row}")
                        continue

                    doc = _build_doc(
                        id=senotypeid,
                        row=senotypejson,
                        session=session,
                        entity_api_url=entity_api_url,
                        token=token,
                        gene_manager=gene_manager,
                    )
                    actual_sha256 = calculate_sha256_hash(doc)
                    if private_sha256s.get(senotypeid) is None:
                        # doc doesn't exist in ES, create it
                        doc["doc_sha256"] = actual_sha256
                        priv_updater.add_update(doc_id=senotypeid, doc=doc, upsert=True)
                    elif private_sha256s[senotypeid] != actual_sha256:
                        # doc exists but has changed, update it in ES
                        doc["doc_sha256"] = actual_sha256
                        priv_updater.add_delete(doc_id=senotypeid)
                        priv_updater.add_update(doc_id=senotypeid, doc=doc, upsert=True)

                except Exception as e:
                    logger.exception(
                        f"Failed to process senotype with id {row.get('senotypeid')}: {e}"
                    )

        logger.info("Finished reindexing senotypes")


def _log_reindex_all_result(future):
    try:
        future.result()
    except Exception:
        logger.exception("Background reindex-all senotypes task failed")


def _build_doc(
    id: str,
    row: str,
    session: Session,
    entity_api_url: str,
    token: str,
    gene_manager: GeneManager,
) -> dict[str, Any]:
    data = json.loads(row)
    doc: dict[str, Any] = {"sennet_id": id}

    senotype = data.get("senotype", {})

    if uuid := senotype.get("uuid"):
        doc["uuid"] = uuid
    if title := senotype.get("name"):
        doc["title"] = title
    if definition := senotype.get("definition"):
        doc["definition"] = definition
    if doi := senotype.get("doi"):
        doc["doi"] = doi

    if provenance := senotype.get("provenance"):
        doc_provenance = {}
        if successor := provenance.get("successor"):
            doc_provenance["successor"] = successor
        if predecessor := provenance.get("predecessor"):
            doc_provenance["predecessor"] = predecessor
        if doc_provenance:
            doc["provenance"] = doc_provenance

    submitter = data.get("submitter", {})
    if name := submitter.get("name"):
        full_name = (name.get("first", "") + " " + name.get("last", "")).strip()
        if full_name:
            doc["created_by_user_displayname"] = full_name
    if email := submitter.get("email"):
        doc["created_by_user_email"] = email

    for assertion in data.get("assertions", []):
        predicate_term = assertion.get("predicate", {}).get("term")
        if not predicate_term:
            continue

        objs = assertion.get("objects", [])

        if predicate_term == "has_sex":
            values = [obj.get("term") for obj in objs if obj.get("term")]
            if values:
                doc["sex"] = values

        elif predicate_term == "has_context":
            # Each object's term field is the measurement name (age, bmi)
            for obj in objs:
                field_name = obj.get("term")
                if not field_name:
                    continue
                keys = ("unit", "value", "lowerbound", "upperbound")
                context = {k: obj[k] for k in keys if k in obj}
                if context:
                    doc[field_name] = context

        elif predicate_term == "has_dataset":
            hgnc_codes = [obj["code"] for obj in objs if obj.get("code", "").startswith("HGNC:")]
            gene_names = gene_manager.get_genes(hgnc_codes) if hgnc_codes else {}
            items = []
            for obj in objs:
                item: dict[str, Any] = {k: obj[k] for k in ("code", "term") if k in obj}
                code = obj.get("code")
                if code:
                    entity = _get_entity(
                        entity_api_url=entity_api_url,
                        entity_id=code,
                        token=token,
                        session=session,
                    )
                    item["uuid"] = entity["uuid"]
                    sleep(0.2)
                    if code in gene_names:
                        item["name"] = gene_names[code]["name"]
                if item:
                    items.append(item)
            if items:
                doc["has_dataset"] = items

        else:
            hgnc_codes = [obj["code"] for obj in objs if obj.get("code", "").startswith("HGNC:")]
            gene_names = gene_manager.get_genes(hgnc_codes) if hgnc_codes else {}
            items = []
            for obj in objs:
                item = {k: obj[k] for k in ("code", "term") if k in obj}
                code = obj.get("code")
                if code and code in gene_names:
                    item["name"] = gene_names[code]["name"]
                if item:
                    items.append(item)
            if items:
                doc[predicate_term] = items

    return doc


def _get_entity(entity_api_url: str, entity_id: str, token: str, session: Session):
    headers = {"Authorization": f"Bearer {token}"}
    res = session.get(f"{entity_api_url}/entities/{entity_id}", headers=headers)
    res.raise_for_status()
    return res.json()
