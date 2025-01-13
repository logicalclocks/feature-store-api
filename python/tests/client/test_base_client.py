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

import os

import pytest
import requests
from hsfs.client.base import Client
from hsfs.client.exceptions import RestAPIError


class TestBaseClient:
    def test_valid_token_no_retires(self, mocker):
        # Arrange
        os.environ[Client.REST_ENDPOINT] = "True"
        client = self._init_test_client()

        path_params = [
            "variables",
            "versions",
        ]

        # Mock the requests library
        mocker.patch("requests.sessions.Session.prepare_request")
        ok_response = requests.Response()
        ok_response.status_code = 200
        ok_response._content = ""
        mocker.patch("requests.sessions.Session.send", return_value=ok_response)

        spy_retry_token_expired = mocker.spy(client, "_retry_token_expired")

        # Act
        client._send_request("GET", path_params)

        # Assert
        assert spy_retry_token_expired.call_count == 0

    def test_invalid_token_retires(self, mocker):
        # Arrange
        os.environ[Client.REST_ENDPOINT] = "True"
        client = self._init_test_client()

        path_params = [
            "variables",
            "versions",
        ]

        # Mock the requests library
        mocker.patch("requests.sessions.Session.prepare_request")
        unauthorized_response = requests.Response()
        unauthorized_response.status_code = 401
        mocker.patch(
            "requests.sessions.Session.send", return_value=unauthorized_response
        )

        # Mock and spy the client
        mocker.patch("hsfs.client.base.Client._read_jwt")
        spy_retry_token_expired = mocker.spy(client, "_retry_token_expired")

        # Act
        with pytest.raises(RestAPIError):
            client._send_request("GET", path_params)

        # Assert
        assert spy_retry_token_expired.call_count == 10

    def test_invalid_token_retires_backoff_break(self, mocker):
        # Arrange
        os.environ[Client.REST_ENDPOINT] = "True"
        client = self._init_test_client()

        path_params = [
            "variables",
            "versions",
        ]

        # Mock the requests library
        mocker.patch("requests.sessions.Session.prepare_request")
        # setup unauthorized response
        unauthorized_response = requests.Response()
        unauthorized_response.status_code = 401

        # setup ok response
        ok_response = requests.Response()
        ok_response.status_code = 200
        ok_response._content = ""

        mocker.patch(
            "requests.sessions.Session.send",
            side_effect=[unauthorized_response] * 5 + [ok_response],
        )

        # Mock and spy the client
        mocker.patch("hsfs.client.base.Client._read_jwt")
        spy_retry_token_expired = mocker.spy(client, "_retry_token_expired")

        # Act
        client._send_request("GET", path_params)

        # Assert
        assert spy_retry_token_expired.call_count == 5

    def _init_test_client(self):
        client = Client()
        client._connected = True
        client._base_url = ""
        client._auth = None
        client._verify = False
        client._session = requests.session()
        client.TOKEN_EXPIRED_RETRY_INTERVAL = 0  # Disable wait for tests

        return client
