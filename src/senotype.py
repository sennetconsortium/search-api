import json
import logging
from concurrent.futures import ThreadPoolExecutor

from atlas_consortia_commons.rest import rest_not_found, rest_server_err
from flask import Blueprint, current_app, g
from mysql.connector.pooling import MySQLConnectionPool

from libs.elasticsearch import ESBulkUpdater, get_docs_from_es
from libs.hash import calculate_sha256_hash
from libs.http import new_session

logger = logging.getLogger()


senotypes_blueprint = Blueprint("senotypes", __name__)


# Auth handled in gateway
@senotypes_blueprint.route("/senotypes/reindex/<string:id>", methods=["PUT"])
def reindex_senotype(id: str):
    # check that the senotypes index configuration exists before proceeding
    search_config = current_app.config["search_config"]
    senotypes_config = search_config.get("indices", {}).get("senotypes")
    if not senotypes_config:
        return rest_server_err("Senotypes index configuration not found")

    # retrieve the senotype record from the database
    with g.db.cursor(dictionary=True) as cursor:
        cursor.execute("SELECT senotypeid, senotypejson FROM senotype WHERE senotypeid = %s", (id,))
        row = cursor.fetchone()

    if row is None:
        return rest_not_found(f"Senotype with id {id} not found")

    id = row["senotypeid"]
    payload = row["senotypejson"]
    if not id or not payload or not isinstance(payload, str):
        logger.warning(f"Invalid data for senotype with id {id}: {row}")
        return rest_server_err(f"Invalid data for senotype with id {id}")

    with ESBulkUpdater(
        es_url=senotypes_config["elasticsearch"]["url"],
        index=senotypes_config["private"],
    ) as priv_updater:
        payload = json.loads(payload)
        actual_sha256 = calculate_sha256_hash(payload)
        payload["doc_sha256"] = actual_sha256
        priv_updater.add_delete(doc_id=id)
        priv_updater.add_update(doc_id=id, doc=payload, upsert=True)

    return {"message": f"Senotype with id {id} reindexed successfully"}, 200


# Auth handled in gateway
@senotypes_blueprint.route("/senotypes/reindex-all", methods=["PUT"])
def reindex_all_senotypes():
    # check that the senotypes index configuration exists before proceeding
    search_config = current_app.config["search_config"]
    senotypes_config = search_config.get("indices", {}).get("senotypes")
    if not senotypes_config:
        return rest_server_err("Senotypes index configuration not found")

    with ThreadPoolExecutor() as executor:
        executor.submit(
            _reindex_senotypes_thread,
            senotypes_config=senotypes_config,
            db_pool=current_app.config["db_pool"],
        )

    return {"message": "Request of reindex all senotypes accepted"}, 201


def _reindex_senotypes_thread(senotypes_config: dict, db_pool: MySQLConnectionPool):
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

    ids = [row["senotypeid"] for row in rows]
    with new_session(senotypes_config["elasticsearch"]["url"]) as session:
        try:
            private_sha256s = {
                doc["_id"]: doc["doc_sha256"]
                for doc in get_docs_from_es(
                    index=senotypes_config["private"],
                    es_url=senotypes_config["elasticsearch"]["url"],
                    fields=["doc_sha256"],
                    session=session,
                    query={"terms": {"_id": ids}},
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
            for row in rows:
                try:
                    id = row["senotypeid"]
                    payload = row["senotypejson"]
                    if not id or not payload or not isinstance(payload, str):
                        logger.warning(f"Skipping row with missing id or payload: {row}")
                        continue
                    payload = json.loads(payload)
                    actual_sha256 = calculate_sha256_hash(payload)

                    if private_sha256s.get(id) is None:
                        # doc doesn't exist in ES, create it
                        payload["doc_sha256"] = actual_sha256
                        priv_updater.add_update(doc_id=id, doc=payload, upsert=True)
                    elif private_sha256s[id] != actual_sha256:
                        # doc exists but has changed, update it in ES
                        payload["doc_sha256"] = actual_sha256
                        priv_updater.add_delete(doc_id=id)
                        priv_updater.add_update(doc_id=id, doc=payload, upsert=True)
                except Exception as e:
                    logger.exception(
                        f"Failed to process senotype with id {row.get('senotypeid')}: {e}"
                    )
                    continue

        logger.info("Finished reindexing senotypes")


@senotypes_blueprint.before_request
def open_db():
    """Open a new database connection before each request for this blueprint."""
    g.db = current_app.config["db_pool"].get_connection()


@senotypes_blueprint.teardown_request
def close_db(error):
    """Return the connection to the pool after each request for this blueprint."""
    db = g.pop("db", None)
    if db is not None:
        db.close()
