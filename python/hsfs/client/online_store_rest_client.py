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
from warnings import warn

from hsfs import client
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core import variable_api

_online_store_rest_client = None


def init_or_reset_online_store_rest_client(
    optional_config: Optional[dict[str, Any]] = None, reset_client: bool = False
):
    global _online_store_rest_client
    if not _online_store_rest_client:
        _online_store_rest_client = OnlineStoreRestClientSingleton(
            optional_config=optional_config
        )
    elif reset_client:
        _online_store_rest_client.reset_client(optional_config=optional_config)
    else:
        warn(
            "Online Store Rest Client is already initialised. To reset connection or/and override configuration, "
            + "use reset_online_store_rest_client or get_instance methods with optional configuration"
        )


def get_instance() -> "OnlineStoreRestClientSingleton":
    global _online_store_rest_client
    if not _online_store_rest_client:
        _online_store_rest_client = OnlineStoreRestClientSingleton()
    return _online_store_rest_client


_DEFAULT_ONLINE_STORE_REST_CLIENT_PORT = 4406
_DEFAULT_ONLINE_STORE_REST_CLIENT_TIMEOUT_MS = 600
_DEFAULT_ONLINE_STORE_REST_CLIENT_VERIFY_CERTS = True
_DEFAULT_ONLINE_STORE_REST_CLIENT_USE_SSL = True
_DEFAULT_ONLINE_STORE_REST_CLIENT_SSL_ASSERT_HOSTNAME = True
_DEFAULT_ONLINE_STORE_REST_CLIENT_SERVER_API_VERSION = "0.1.0"
_DEFAULT_ONLINE_STORE_REST_CLIENT_HTTP_AUTHORIZATION = "X-API-KEY"


class OnlineStoreRestClientSingleton:
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
        self._session = None
        self._current_config = None
        self._setup_rest_client(
            optional_config=optional_config, use_current_config=False
        )
        self.is_connected()

    def reset_client(self, optional_config: Optional[dict[str, Any]] = None):
        self._check_hopsworks_connection()
        if self._session is not None:
            self._session.close()
        self._session = None
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
            self._current_config = self._get_default_client_config()
        if optional_config:
            self._current_config.update(optional_config)

        self._set_auth(optional_config)
        self._verify = self._current_config[self.VERIFY_CERTS]
        if not self._session:
            self._session = requests.Session()
        else:
            raise ValueError(
                "Use the init_or_reset_online_store_connection method with reset_connection flag set "
                + "to True to reset the online_store_client_connection"
            )

        # Set base_url
        scheme = "https" if self._current_config[self.USE_SSL] else "http"
        self._base_url = furl(
            f"{scheme}://{self._current_config[self.HOST]}:{self._current_config[self.PORT]}/{self._current_config[self.SERVER_API_VERSION]}"
        )

        assert (
            self._session is not None
        ), "Online Store REST Client failed to initialise."
        assert (
            self._auth is not None
        ), "Online Store REST Client Authentication failed to initialise. Check API Key."
        assert (
            self._base_url is not None
        ), "Online Store REST Client Base URL failed to initialise. Check host and port parameters."
        assert (
            self._current_config is not None
        ), "Online Store REST Client Configuration failed to initialise."

    def _get_default_client_config(self) -> dict[str, Any]:
        default_config = self._get_default_static_parameters_config()
        default_config.update(self._get_default_dynamic_parameters_config())
        return default_config

    def _get_default_static_parameters_config(self) -> dict[str, Any]:
        return {
            self.TIMEOUT: _DEFAULT_ONLINE_STORE_REST_CLIENT_TIMEOUT_MS,
            self.VERIFY_CERTS: _DEFAULT_ONLINE_STORE_REST_CLIENT_VERIFY_CERTS,
            self.USE_SSL: _DEFAULT_ONLINE_STORE_REST_CLIENT_USE_SSL,
            self.SSL_ASSERT_HOSTNAME: _DEFAULT_ONLINE_STORE_REST_CLIENT_SSL_ASSERT_HOSTNAME,
            self.SERVER_API_VERSION: _DEFAULT_ONLINE_STORE_REST_CLIENT_SERVER_API_VERSION,
            self.HTTP_AUTHORIZATION: _DEFAULT_ONLINE_STORE_REST_CLIENT_HTTP_AUTHORIZATION,
        }

    def _get_default_dynamic_parameters_config(
        self,
    ) -> dict[str, Any]:
        url = furl(self._get_rondb_rest_server_endpoint())
        return {
            self.HOST: url.host,
            self.PORT: url.port,
            self.CA_CERTS: client.get_instance()._get_ca_chain_path(),
        }

    def _get_rondb_rest_server_endpoint(self) -> str:
        if client.get_instance()._is_external():
            external_domain = self.variable_api.get_loadbalancer_external_domain()
            if external_domain == "":
                external_domain = client.get_instance().host
            return f"https://{external_domain}:{_DEFAULT_ONLINE_STORE_REST_CLIENT_PORT}"
        else:
            service_discovery_domain = self.variable_api.get_service_discovery_domain()
            if service_discovery_domain == "":
                raise FeatureStoreException("Service discovery domain is not set.")
            return f"https://rdrs.service.{service_discovery_domain}:{_DEFAULT_ONLINE_STORE_REST_CLIENT_PORT}"

    def send_request(
        self,
        method: str,
        path_params: list[str],
        headers: Optional[dict[str, Any]] = None,
        data: Optional[dict[str, Any]] = None,
    ):
        url = self._base_url.copy()
        url.path.segments.extend(path_params)
        prepped_request = self._session.prepare_request(
            requests.Request(
                method, url=url.url, headers=headers, data=data, auth=self._auth
            )
        )
        response = self._session.send(
            prepped_request,
            verify=self._current_config[self.VERIFY_CERTS],
            timeout=self._current_config[self.TIMEOUT] / 1000,
        )
        return response

    def _check_hopsworks_connection(self):
        assert (
            client.get_instance() is not None and client.get_instance()._connected
        ), """Hopsworks Client is not connected. Please connect to Hopsworks cluster
            via hopsworks.login or hsfs.connection before initialising the Online Store REST Client.
            """

    def _set_auth(self, optional_config: Optional[dict[str, Any]] = None):
        if client.get_instance()._is_external():
            assert hasattr(
                client.get_instance()._auth, "_token"
            ), "External client must use API Key authentication. Contact your system administrator."
            self._auth = client.auth.OnlineStoreKeyAuth(
                client.get_instance()._auth._token
            )
        elif isinstance(optional_config, dict) and optional_config.get(
            self.API_KEY, False
        ):
            self._auth = client.auth.OnlineStoreKeyAuth(optional_config[self.API_KEY])
        elif self._auth is not None:
            return
        else:
            raise FeatureStoreException(
                "RonDB Rest Server uses Hopsworks Api Key to authenticate request."
                + f"Provide a configuration with the {self.API_KEY} key."
            )

    def is_connected(self):
        if self._session is None:
            raise FeatureStoreException("Online Store REST Client is not initialised.")

        if not self.send_request("GET", ["ping"]):
            warn("Ping failed, RonDB Rest Server is not reachable.")
            return False
        return True
