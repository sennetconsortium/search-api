import logging
from pathlib import Path

import psutil
import requests
from flask import Blueprint, jsonify
from translator.progress_interface import ProgressReadInterface

logger = logging.getLogger()


def create_blueprint(config: dict, progress_interface: ProgressReadInterface):
    status_blueprint = Blueprint("status", __name__)

    @status_blueprint.route("/status", methods=["GET"])
    def get_status():
        response_code = 200
        try:
            file_version_content = (
                (Path(__file__).absolute().parent.parent / "VERSION").read_text().strip()
            )
        except Exception as e:
            file_version_content = str(e)
            response_code = 500

        try:
            file_build_content = (
                (Path(__file__).absolute().parent.parent / "BUILD").read_text().strip()
            )
        except Exception as e:
            file_build_content = str(e)
            response_code = 500

        status_data = {
            "version": file_version_content,
            "build": file_build_content,
            "usage": [],
            "services": [],
        }

        # Usage
        try:
            # get memory usage
            memory_percent = (
                100
                * (psutil.virtual_memory().total - psutil.virtual_memory().free)
                / psutil.virtual_memory().total
            )
            status_data["usage"].append(
                {
                    "type": "memory",
                    "percent_used": round(memory_percent, 1),
                    "description": "host memory",
                }
            )

            # get disk usage
            disks = config.get("STATUS_DISKS", {})
            for name, description in disks.items():
                disk_usage = psutil.disk_usage(name)
                storage_percent = (disk_usage.used / disk_usage.total) * 100
                status_data["usage"].append(
                    {
                        "type": "storage",
                        "percent_used": round(storage_percent, 1),
                        "description": description,
                    }
                )
        except Exception as e:
            response_code = 500
            logger.error(f"Error getting system usage: {str(e)}")

        # check the elasticsearch connection
        try:
            service = {"name": "elasticsearch", "status": True}
            es_url = config["DEFAULT_ELASTICSEARCH_URL"] + "/_cluster/health"
            res = requests.get(url=es_url)
            if res.status_code != 200:
                raise Exception(
                    f"Cannot connect to Elasticsearch server at {config['DEFAULT_ELASTICSEARCH_URL']}"
                )
            if res.json().get("status") != "green":
                raise Exception(
                    f"Elasticsearch server at {config['DEFAULT_ELASTICSEARCH_URL']} is not healthy"
                )
        except Exception as e:
            service["status"] = False
            service["message"] = str(e).replace("'", "")
            response_code = 500
        status_data["services"].append(service)

        # check the memcached connection
        if progress_interface:
            try:
                service = {"name": "memcached", "status": True}
                index_process = {
                    "is_indexing": progress_interface.is_indexing,
                    "percent_complete": progress_interface.percent_complete,
                }
            except Exception as e:
                service["status"] = False
                service["message"] = str(e).replace("'", "")
                index_process = {"is_indexing": False, "percent_complete": 0}
                response_code = 500

            status_data["services"].append(service)
            status_data["indexing"] = index_process

        return jsonify(status_data), response_code

    return status_blueprint
