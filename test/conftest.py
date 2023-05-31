def pytest_addoption(parser):
    parser.addoption("--es_url", action="store", help="Elasticsearch URL")

def pytest_collection_modifyitems(items, config):
    """Modifies test items in place to ensure test classes run in a given order"""
    test_order = [
        'test_create_index',
        'test_create_document',
        'test_update_document',
        'test_delete_document',
        'test_delete_index'
    ]

    sorted_items = []
    for test in test_order:
        sorted_items += [item for item in items if item.name == test]

    items[:] = sorted_items
