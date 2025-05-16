import importlib
import json
import time

import pytest
import requests

es_writer_module = importlib.import_module("search-adaptor.src.libs.es_writer")

### Test params

test_index_name = "test_index"
test_doc_id = "5adea629a91c4b989723c48af1470e86"

### Fixtures


@pytest.fixture(scope="session")
def es_url(pytestconfig):
    return pytestconfig.getoption("es_url")


@pytest.fixture
def es_writer(es_url):
    yield es_writer_module.ESWriter(es_url)
    # Rate limit to avoid AWS ES throttling
    time.sleep(0.5)


@pytest.fixture(scope="session", autouse=True)
def cleanup(es_url):
    yield True
    # Called after all tests are run. Clean up any indexes or documents created
    # during testing just in case the tests fail.
    requests.delete(f"{es_url}/{test_index_name}")
    requests.delete(f"{es_url}/{test_index_name}/_doc/{test_doc_id}")


### Helper functions


def get_index(es_url, index_name):
    headers = {"Content-Type": "application/json"}
    rspn = requests.get(f"{es_url}/{index_name}", headers=headers)
    return rspn.json()


def get_document(es_url, index_name, uuid):
    headers = {"Content-Type": "application/json"}
    rspn = requests.get(f"{es_url}/{index_name}/_doc/{uuid}", headers=headers)
    return rspn.json()


### Tests

# Order of tests is specified in the pytest_collection_modifyitems function
# in test/conftest.py. Running pytests in this order limits the number of calls
# to the AWS ES service. This is not the best practice for testing but it works.


def test_create_index(es_writer):
    """Test that the create index method creates an index"""
    es_writer.create_index(test_index_name, {})

    index = get_index(es_writer.elasticsearch_url, test_index_name)
    assert index.get(test_index_name) is not None


def test_create_document(es_writer):
    """Test that the create document method creates a document"""
    doc = {"uuid": test_doc_id, "test": f"test {test_doc_id}"}
    es_writer.write_document(test_index_name, json.dumps(doc), test_doc_id)

    document = get_document(es_writer.elasticsearch_url, test_index_name, test_doc_id)
    assert document["_source"] == doc


def test_update_document(es_writer):
    """Test that the update document method updates a document"""
    doc = {"uuid": test_doc_id, "test": f"test update {test_doc_id}"}
    es_writer.write_or_update_document(test_index_name, doc=json.dumps(doc), uuid=test_doc_id)

    document = get_document(es_writer.elasticsearch_url, test_index_name, test_doc_id)
    assert document["_source"] == doc


def test_delete_document(es_writer):
    """Test that the delete document method deletes a document"""
    es_writer.delete_document(test_index_name, test_doc_id)

    document = get_document(es_writer.elasticsearch_url, test_index_name, test_doc_id)
    assert document.get("found") is False


def test_delete_index(es_writer):
    """Test that the delete index method delete an index"""
    es_writer.delete_index(test_index_name)

    index = get_index(es_writer.elasticsearch_url, test_index_name)
    assert index.get("error") is not None
    assert index["status"] == 404
