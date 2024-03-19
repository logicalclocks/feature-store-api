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
import json

import pytest
import requests
from hsfs.core import online_store_rest_client_api


class TestOnlineStoreRestClientApi:
    def test_handle_rdrs_feature_store_response_bad_request_primary_key_or_passed_features(
        self, backend_fixtures
    ):
        # Arrange
        response = requests.Response()
        response.status_code = 400
        response._content = json.dumps(
            backend_fixtures["rondb_server"][
                "bad_request_primary_key_or_passed_features_error"
            ]
        ).encode("utf-8")
        online_rest_api = online_store_rest_client_api.OnlineStoreRestClientApi()

        # Act
        with pytest.raises(online_store_rest_client_api.exceptions.RestAPIError):
            online_rest_api.handle_rdrs_feature_store_response(response)

    def test_handle_rdrs_feature_store_response_bad_request_metadata(
        self, backend_fixtures
    ):
        # Arrange
        response = requests.Response()
        response.status_code = 400
        response._content = json.dumps(
            backend_fixtures["rondb_server"]["bad_request_feature_store_view_not_exist"]
        ).encode("utf-8")
        online_rest_api = online_store_rest_client_api.OnlineStoreRestClientApi()

        # Act
        with pytest.raises(online_store_rest_client_api.exceptions.RestAPIError):
            online_rest_api.handle_rdrs_feature_store_response(response)

    def test_handle_rdrs_feature_store_response_unauthorized_request_error(
        self, backend_fixtures
    ):
        # Arrange
        response = requests.Response()
        response.status_code = 401
        response._content = json.dumps(
            backend_fixtures["rondb_server"]["unauthorized_request_error"]
        ).encode("utf-8")
        online_rest_api = online_store_rest_client_api.OnlineStoreRestClientApi()

        # Act
        with pytest.raises(online_store_rest_client_api.exceptions.RestAPIError):
            online_rest_api.handle_rdrs_feature_store_response(response)

    def test_handle_rdrs_feature_store_response_internal_server_error(
        self, backend_fixtures
    ):
        # Arrange
        response = requests.Response()
        response.status_code = 500
        response._content = json.dumps(
            backend_fixtures["rondb_server"]["internal_server_error"]
        ).encode("utf-8")
        online_rest_api = online_store_rest_client_api.OnlineStoreRestClientApi()

        # Act
        with pytest.raises(online_store_rest_client_api.exceptions.RestAPIError):
            online_rest_api.handle_rdrs_feature_store_response(response)
