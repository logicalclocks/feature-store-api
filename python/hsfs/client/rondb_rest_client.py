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
import requests
from furl import furl
from typing import Optional, Any

from hsfs import client
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core import variable_api

_rondb_client = None


def init_rondb_rest_client(optional_config: Optional[dict[str, Any]] = None):
    assert (
        client.get_instance() is not None
    ), """Hopsworks Client is not connected. Please connect to Hopsworks cluster
            via hopsworks.login or hsfs.connection before initialising the RonDB REST Client.
            """

    global _rondb_client
    if not _rondb_client:
        _rondb_client = RondbRestClientSingleton(optional_config=optional_config)
    else:
        _rondb_client.refresh_rondb_connection(optional_config=optional_config)


def get_instance() -> "RondbRestClientSingleton":
    global _rondb_client
    if not _rondb_client:
        _rondb_client = RondbRestClientSingleton()
    return _rondb_client


_DEFAULT_RONDB_REST_CLIENT_PORT = 4406
_DEFAULT_RONDB_REST_CLIENT_TIMEOUT_MS = 600
_DEFAULT_RONDB_REST_CLIENT_VERIFY_CERTS = True
_DEFAULT_RONDB_REST_CLIENT_USE_SSL = True
_DEFAULT_RONDB_REST_CLIENT_SSL_ASSERT_HOSTNAME = True
_DEFAULT_RONDB_REST_CLIENT_SERVER_API_VERSION = "0.1.0"
_DEFAULT_RONDB_REST_CLIENT_HTTP_AUTHORIZATION = "X-API-KEY"


class RondbRestClientSingleton:
    HOST = "host"
    PORT = "port"
    VERIFY_CERTS = "verify_certs"
    USE_SSL = "use_ssl"
    SSL_ASSERT_HOSTNAME = "ssl_assert_hostname"
    CA_CERTS = "ca_certs"
    HTTP_COMPRESS = "http_compress"
    HTTP_AUTHORIZATION = "http_authorization"
    TIMEOUT = "timeout"
    SERVER_API_VERSION = "server_api_version"
    API_KEY = "api_key"

    def __init__(self, optional_config: dict[str, Any] = None):
        self._check_hopsworks_connection()
        self.variable_api = variable_api.VariableApi()
        self._auth = None
        self._rest_client = None
        self._current_config = None
        self._setup_rest_client(
            optional_config=optional_config, use_current_config=False
        )
        # self._check_rondb_connection()

    def refresh_rondb_connection(
        self, optional_config: Optional[dict[str, Any]] = None
    ):
        self._check_hopsworks_connection()
        if self._rest_client is not None:
            self._rest_client.close()
        self._rest_client = None
        self._setup_rest_client(
            optional_config=optional_config,
            use_current_config=False if optional_config else True,
        )

    def _setup_rest_client(
        self,
        optional_config: Optional[dict[str, Any]] = None,
        use_current_config: bool = True,
    ):
        if optional_config and not isinstance(optional_config, dict):
            raise ValueError(
                "optional_config must be a dictionary. See documentation for allowed keys and values."
            )
        if not use_current_config:
            self._current_config = self._get_default_rondb_rest_client_config()
        if optional_config:
            self._current_config.update(optional_config)

        self._set_auth(optional_config)
        self._verify = self._current_config[self.VERIFY_CERTS]
        if not self._rest_client:
            self._rest_client = requests.Session()
        else:
            raise ValueError(
                "Use the refresh_rondb_connection method to reset the rondb_client_connection"
            )

        # Set base_url
        scheme = "https" if self._current_config[self.USE_SSL] else "http"
        self._base_url = furl(
            f"{scheme}://{self._current_config[self.HOST]}:{self._current_config[self.PORT]}/{self._current_config[self.SERVER_API_VERSION]}"
        )

        assert self._rest_client is not None, "RonDB Rest Client failed to initialise."
        assert (
            self._auth is not None
        ), "RonDB Rest Client Authentication failed to initialise. Check API Key."
        assert (
            self._base_url is not None
        ), "RonDB Rest Client Base URL failed to initialise. Check host and port parameters."
        assert (
            self._current_config is not None
        ), "RonDB Rest Client Configuration failed to initialise."

    def _get_default_rondb_rest_client_config(self) -> dict[str, Any]:
        default_config = self._get_default_rondb_rest_client_static_parameters_config()
        default_config.update(
            self._get_default_rondb_rest_client_dynamic_parameters_config()
        )
        return default_config

    def _get_default_rondb_rest_client_static_parameters_config(self) -> dict[str, Any]:
        return {
            self.TIMEOUT: _DEFAULT_RONDB_REST_CLIENT_TIMEOUT_MS,
            self.VERIFY_CERTS: _DEFAULT_RONDB_REST_CLIENT_VERIFY_CERTS,
            self.USE_SSL: _DEFAULT_RONDB_REST_CLIENT_USE_SSL,
            self.SSL_ASSERT_HOSTNAME: _DEFAULT_RONDB_REST_CLIENT_SSL_ASSERT_HOSTNAME,
            self.SERVER_API_VERSION: _DEFAULT_RONDB_REST_CLIENT_SERVER_API_VERSION,
            self.HTTP_AUTHORIZATION: _DEFAULT_RONDB_REST_CLIENT_HTTP_AUTHORIZATION,
        }

    def _get_default_rondb_rest_client_dynamic_parameters_config(
        self,
    ) -> dict[str, Any]:
        url = furl(self._get_rondb_rest_server_endpoint())
        return {
            self.HOST: url.host,
            self.PORT: url.port,
            self.CA_CERTS: client.get_instance()._get_ca_chain_path(),
        }

    def _get_rondb_rest_server_endpoint(self) -> str:
        if isinstance(client.get_instance(), client.external.Client):
            external_domain = self.variable_api.get_loadbalancer_external_domain()
            if external_domain == "":
                external_domain = client.get_instance().host
            return f"https://{external_domain}:{_DEFAULT_RONDB_REST_CLIENT_PORT}"
        else:
            service_discovery_domain = self.variable_api.get_service_discovery_domain()
            if service_discovery_domain == "":
                raise FeatureStoreException("Service discovery domain is not set.")
            return f"https://rdrs.service.{service_discovery_domain}:{_DEFAULT_RONDB_REST_CLIENT_PORT}"

    def _send_request(
        self,
        method: str,
        path_params: list[str],
        headers: Optional[dict[str, Any]] = None,
        data: Optional[dict[str, Any]] = None,
    ):
        url = self._base_url.copy()
        url.path.segments.extend(path_params)
        prepped_request = self._rest_client.prepare_request(
            requests.Request(
                method, url=url.url, headers=headers, data=data, auth=self._auth
            )
        )
        response = self._rest_client.send(
            prepped_request,
            verify=self._current_config[self.VERIFY_CERTS],
            timeout=self._current_config[self.TIMEOUT] / 1000,
        )
        return response

    def _check_hopsworks_connection(self):
        assert (
            client.get_instance() is not None and client.get_instance()._connected
        ), """Hopsworks Client is not connected. Please connect to Hopsworks cluster
            via hopsworks.login or hsfs.connection before initialising the RonDB REST Client.
            """

    def _set_auth(self, optional_config: Optional[dict[str, Any]] = None):
        if isinstance(client.get_instance(), client.external.Client):
            assert isinstance(
                client.get_instance()._auth, client.auth.ApiKeyAuth
            ), "External client must use API Key authentication. Contact your system administrator."
            self._auth = client.auth.RonDBKeyAuth(client.get_instance()._auth._token)
        elif isinstance(optional_config, dict) and optional_config.get(
            self.API_KEY, False
        ):
            self._auth = client.auth.RonDBKeyAuth(optional_config[self.API_KEY])
        elif self._auth is not None:
            return
        else:
            raise FeatureStoreException(
                "RonDB Rest Server uses Hopsworks Api Key to authenticate request."
                + f"Provide a configuration with the {self.API_KEY} key."
            )

    def _check_rondb_connection(self):
        if self._rest_client is None:
            raise FeatureStoreException("RonDB Rest Client is not initialised.")
        else:
            assert self._send_request(
                "GET", ["ping"]
            ), "RonDB Rest Server is not reachable."
