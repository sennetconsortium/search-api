import importlib
import os
import sys
from pathlib import Path
import logging
from flask import Flask
from yaml import safe_load

# Atlas Consortia commons
from atlas_consortia_commons.ubkg import initialize_ubkg
from atlas_consortia_commons.rest import *
from atlas_consortia_commons.ubkg.ubkg_sdk import init_ontology

sys.path.append("search-adaptor/src")
search_adaptor_module = importlib.import_module("app", "search-adaptor/src")

# Root logger configuration
global logger

# Set logging format and level (default is warning)
logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')

# Use `getLogger()` instead of `getLogger(__name__)` to apply the config to the root logger
# will be inherited by the sub-module loggers
logger = logging.getLogger()

config = {}
app = Flask(__name__, instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__)), 'instance'),
            instance_relative_config=True)
app.config.from_pyfile('app.cfg')
_config = app.config

# load the index configurations and set the default
config['INDICES'] = safe_load((Path(__file__).absolute().parent / 'instance/search-config.yaml').read_text())
config['DEFAULT_INDEX_WITHOUT_PREFIX'] = config['INDICES']['default_index']

# Remove trailing slash / from URL base to avoid "//" caused by config with trailing slash
config['DEFAULT_ELASTICSEARCH_URL'] = config['INDICES']['indices'][config['DEFAULT_INDEX_WITHOUT_PREFIX']]['elasticsearch']['url'].strip('/')
config['DEFAULT_ENTITY_API_URL'] = config['INDICES']['indices'][config['DEFAULT_INDEX_WITHOUT_PREFIX']]['document_source_endpoint'].strip('/')

config['APP_CLIENT_ID'] = app.config['APP_CLIENT_ID']
config['APP_CLIENT_SECRET'] = app.config['APP_CLIENT_SECRET']

config['AWS_ACCESS_KEY_ID'] = app.config['AWS_ACCESS_KEY_ID']
config['AWS_SECRET_ACCESS_KEY'] = app.config['AWS_SECRET_ACCESS_KEY']
config['AWS_S3_BUCKET_NAME'] = app.config['AWS_S3_BUCKET_NAME']
config['AWS_S3_OBJECT_PREFIX'] = app.config['AWS_S3_OBJECT_PREFIX']
config['AWS_OBJECT_URL_EXPIRATION_IN_SECS'] = app.config['AWS_OBJECT_URL_EXPIRATION_IN_SECS']
config['LARGE_RESPONSE_THRESHOLD'] = app.config['LARGE_RESPONSE_THRESHOLD']

translator_module = importlib.import_module("sennet_translator")

sys.path.append("libs")


# This `app` will be imported by wsgi.py when deployed with uWSGI server
search_api_instance = search_adaptor_module.SearchAPI(config, translator_module)
app = search_api_instance.app

####################################################################################################
## UBKG Ontology and REST initialization
####################################################################################################

try:
    for exception in get_http_exceptions_classes():
        app.register_error_handler(exception, abort_err_handler)
    app.ubkg = initialize_ubkg(_config)
    search_api_instance.ubkg_instance = app.ubkg
    with app.app_context():
        init_ontology()

    logger.info("Initialized ubkg module successfully :)")
# Use a broad catch-all here
except Exception:
    msg = "Failed to initialize the ubkg module"
    # Log the full stack trace, prepend a line with our message
    logger.exception(msg)

# For local standalone (non-docker) development/testing
if __name__ == "__main__":
    app.run(host='0.0.0.0', port="5005")
