import pytest
import json

with open('tests/fixtures/backend_fixtures.json', 'r') as json_file:
    backend_fixtures = json.load(json_file)

@pytest.fixture
def response_get_feature_group():
    return backend_fixtures["get_feature_group"]["response"]