#
#   Copyright 2024 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
# from unittest.mock import patch
import pytest
from furl import furl

import hsfs
from hsfs.client import rondb_rest_client, exceptions, auth


class MockExternalClient:
    def __init__(self):
        self._connected = True
        self._auth = auth.ApiKeyAuth("external_client_api_key")

    def _is_external(self):
        return True

    def _get_ca_chain_path(self):
        return "/tmp/ca_chain.pem"


class MockInternalClient:
    def __init__(self):
        self._connected = True

    def _is_external(self):
        return False

    def _get_ca_chain_path(self):
        return "/tmp/ca_chain.pem"


class TestRondbRestClient:
    def test_setup_rondb_rest_client_external_2(self, mocker, monkeypatch):
        # Arrange
        rondb_rest_client._rondb_client = None

        def client_get_instance():
            return MockExternalClient()

        monkeypatch.setattr(hsfs.client, "get_instance", client_get_instance)
        variable_api_mock = mocker.patch(
            "hsfs.core.variable_api.VariableApi.get_loadbalancer_external_domain",
            return_value="app.hopsworks.ai",
        )

        # Act
        rondb_rest_client.init_rondb_rest_client()
        rondb_rest_client_instance = rondb_rest_client.get_instance()

        # Assert
        variable_api_mock.assert_called_once()
        assert rondb_rest_client_instance._current_config["host"] == "app.hopsworks.ai"
        assert rondb_rest_client_instance._current_config["port"] == 4406
        assert rondb_rest_client_instance._current_config["verify_certs"] is True
        assert rondb_rest_client_instance._base_url == furl(
            "https://app.hopsworks.ai:4406/0.1.0"
        )
        assert rondb_rest_client_instance._auth._token == "external_client_api_key"

    def test_setup_rondb_rest_client_internal(self, mocker, monkeypatch):
        # Arrange
        rondb_rest_client._rondb_client = None

        def client_get_instance():
            return MockInternalClient()

        monkeypatch.setattr(hsfs.client, "get_instance", client_get_instance)
        variable_api_mock = mocker.patch(
            "hsfs.core.variable_api.VariableApi.get_service_discovery_domain",
            return_value="consul",
        )
        optional_config = {"api_key": "provided_api_key"}

        # Act
        with pytest.raises(exceptions.FeatureStoreException):
            rondb_rest_client.init_rondb_rest_client()
        rondb_rest_client.init_rondb_rest_client(optional_config=optional_config)
        rondb_rest_client_instance = rondb_rest_client.get_instance()

        # Assert
        assert variable_api_mock.call_count == 2
        assert (
            rondb_rest_client_instance._current_config["host"] == "rdrs.service.consul"
        )
        assert rondb_rest_client_instance._current_config["port"] == 4406
        assert rondb_rest_client_instance._current_config["verify_certs"] is True
        assert rondb_rest_client_instance._base_url == furl(
            "https://rdrs.service.consul:4406/0.1.0"
        )
        assert rondb_rest_client_instance._auth._token == "provided_api_key"
