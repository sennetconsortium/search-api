import json
import logging

from atlas_consortia_commons.rest import rest_not_found, rest_server_err
from flask import Blueprint, current_app, g

from libs.elasticsearch import ESBulkUpdater

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

    payload = json.loads(payload)
    with ESBulkUpdater(
        es_url=senotypes_config["elasticsearch"]["url"],
        index=senotypes_config["private"],
    ) as priv_updater:
        priv_updater.add_delete(id)
        priv_updater.add_create(id, payload)

    return {"message": f"Senotype with id {id} reindexed successfully"}, 200


# Auth handled in gateway
@senotypes_blueprint.route("/senotypes/reindex-all", methods=["PUT"])
def reindex_all_senotypes():
    # check that the senotypes index configuration exists before proceeding
    search_config = current_app.config["search_config"]
    senotypes_config = search_config.get("indices", {}).get("senotypes")
    if not senotypes_config:
        return rest_server_err("Senotypes index configuration not found")

    # retrieve all senotype records from the database
    with g.db.cursor(dictionary=True) as cursor:
        cursor.execute("SELECT senotypeid, senotypejson FROM senotype")
        rows = cursor.fetchall()

    with ESBulkUpdater(
        es_url=senotypes_config["elasticsearch"]["url"],
        index=senotypes_config["private"],
    ) as priv_updater:
        for row in rows:
            id = row["senotypeid"]
            payload = row["senotypejson"]
            if not id or not payload or not isinstance(payload, str):
                logger.warning(f"Skipping row with missing id or payload: {row}")
                continue
            payload = json.loads(payload)
            priv_updater.add_delete(id)
            priv_updater.add_create(id, payload)

    return {"message": "All senotypes reindexed successfully"}, 200


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
