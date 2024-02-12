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

from hsfs.client import client, FeatureStoreException
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
        _rondb_client = RondbRestClientSingleton(optional_config)
    else:
        _rondb_client._refresh_rondb_connection()


def get_instance() -> "RondbRestClientSingleton":
    global _rondb_client
    if not _rondb_client:
        _rondb_client = RondbRestClientSingleton()
    return _rondb_client


_DEFAULT_RONDB_REST_CLIENT_PORT = 4406
_DEFAULT_RONDB_REST_CLIENT_TIMEOUT_MS = 60
_DEFAULT_RONDB_REST_CLIENT_VERIFY_CERTS = True
_DEFAULT_RONDB_REST_CLIENT_USE_SSL = True
_DEFAULT_RONDB_REST_CLIENT_SSL_ASSERT_HOSTNAME = True
_DEFAULT_RONDB_REST_CLIENT_SERVER_API_VERSION = "0.1.0"
_DEFAULT_RONDB_REST_CLIENT_HTTP_AUTHORIZATION = "X-API-KEY"


class RONDBRESTCLIENT_CONFIG:
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


class RondbRestClientSingleton:
    def __init__(self, optional_config: dict[str, Any] = None):
        self.variable_api = variable_api.VariableApi()
        default_config = self.get_default_rondb_rest_client_config()

        if optional_config and not isinstance(optional_config, dict):
            raise ValueError(
                "optional_config must be a dictionary. See documentation for allowed keys and values."
            )
        elif optional_config:
            default_config.update(optional_config)
        self._current_config = default_config
        self._setup_rest_client(full_config=default_config)

    def _refresh_rondb_connection(self):
        self._rest_client.close()
        self._rest_client = None
        self._setup_rest_client(self._current_config)

    def _setup_rest_client(self, full_config: dict[str, Any]):
        if not self._rest_client:
            self._rest_client = requests.session.Session()
        else:
            raise ValueError(
                "Use the _refresh_rondb_connection to reset the rondb_client_connection"
            )

        # Auth via Hopsworks ApiKey supported by featurestore and batch_featurestore endpoints
        self._rest_client.headers.update(
            {
                full_config[RONDBRESTCLIENT_CONFIG.HTTP_AUTHORIZATION]: full_config[
                    RONDBRESTCLIENT_CONFIG.API_KEY
                ]
            }
        )

        # Both endpoints support only json payloads
        self._rest_client.headers.update({"Content-Type": "application/json"})

        # Set base_url
        scheme = "https" if full_config[RONDBRESTCLIENT_CONFIG.USE_SSL] else "http"
        self._base_url = furl.furl(
            f"{scheme}://{full_config[RONDBRESTCLIENT_CONFIG.HOST]}:{full_config[RONDBRESTCLIENT_CONFIG.PORT]}/{full_config[RONDBRESTCLIENT_CONFIG.SERVER_API_VERSION]}"
        )

    def _get_default_rondb_rest_client_config(self) -> dict[str, Any]:
        default_config = self.get_default_rondb_rest_client_static_parameters_config()
        default_config.update(
            self.get_default_rondb_rest_client_dynamic_parameters_config()
        )
        return default_config

    def _get_default_rondb_rest_client_static_parameters_config(self) -> dict[str, Any]:
        return {
            RONDBRESTCLIENT_CONFIG.TIMEOUT: _DEFAULT_RONDB_REST_CLIENT_TIMEOUT_MS,
            RONDBRESTCLIENT_CONFIG.VERIFY_CERTS: _DEFAULT_RONDB_REST_CLIENT_VERIFY_CERTS,
            RONDBRESTCLIENT_CONFIG.USE_SSL: _DEFAULT_RONDB_REST_CLIENT_USE_SSL,
            RONDBRESTCLIENT_CONFIG.SSL_ASSERT_HOSTNAME: _DEFAULT_RONDB_REST_CLIENT_SSL_ASSERT_HOSTNAME,
            RONDBRESTCLIENT_CONFIG.SERVER_API_VERSION: _DEFAULT_RONDB_REST_CLIENT_SERVER_API_VERSION,
            RONDBRESTCLIENT_CONFIG.HTTP_AUTHORIZATION: _DEFAULT_RONDB_REST_CLIENT_HTTP_AUTHORIZATION,
        }

    def _get_default_rondb_rest_client_dynamic_parameters_config(
        self,
    ) -> dict[str, Any]:
        url = furl(self._get_rondb_rest_server_endpoint())
        return {
            RONDBRESTCLIENT_CONFIG.HOST: url.host,
            RONDBRESTCLIENT_CONFIG.PORT: url.port,
            RONDBRESTCLIENT_CONFIG.CA_CERTS: client.get_instance()._get_ca_certs(),
            RONDBRESTCLIENT_CONFIG.API_KEY: client.get_instance()._read_api_key(),
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
            requests.Request(method, url, headers=headers, data=data)
        )
        response = self._rest_client.send(
            prepped_request,
            verify=self._current_config[RONDBRESTCLIENT_CONFIG.VERIFY_CERTS],
            timeout=self._current_config[RONDBRESTCLIENT_CONFIG.TIMEOUT] / 1000,
        )
        response.raise_for_status()
        return response.json()
