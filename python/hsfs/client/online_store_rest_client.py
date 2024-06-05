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
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Union
from warnings import warn

import requests
import requests.adapters
from furl import furl
from hsfs import client
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core import variable_api


_logger = logging.getLogger(__name__)

_online_store_rest_client = None


def init_or_reset_online_store_rest_client(
    transport: Optional[
        Union[requests.adapters.HTTPAdapter, requests.adapters.BaseAdapter]
    ] = None,
    optional_config: Optional[Dict[str, Any]] = None,
    reset_client: bool = False,
):
    global _online_store_rest_client
    if not _online_store_rest_client:
        _online_store_rest_client = OnlineStoreRestClientSingleton(
            transport=transport, optional_config=optional_config
        )
    elif reset_client:
        _online_store_rest_client.reset_client(
            transport=transport, optional_config=optional_config
        )
    else:
        _logger.warning(
            "Online Store Rest Client is already initialised. To reset connection or/and override configuration, "
            + "use reset_online_store_rest_client flag.",
            stacklevel=2,
        )


def get_instance() -> OnlineStoreRestClientSingleton:
    global _online_store_rest_client
    if _online_store_rest_client is None:
        _logger.warning(
            "Online Store Rest Client is not initialised. Initialising with default configuration."
        )
        _online_store_rest_client = OnlineStoreRestClientSingleton()
    _logger.debug("Accessing global Online Store Rest Client instance.")
    return _online_store_rest_client


class OnlineStoreRestClientSingleton:
    HOST = "host"
    PORT = "port"
    VERIFY_CERTS = "verify_certs"
    USE_SSL = "use_ssl"
    CA_CERTS = "ca_certs"
    HTTP_AUTHORIZATION = "http_authorization"
    TIMEOUT = "timeout"
    SERVER_API_VERSION = "server_api_version"
    API_KEY = "api_key"
    _DEFAULT_ONLINE_STORE_REST_CLIENT_PORT = 4406
    _DEFAULT_ONLINE_STORE_REST_CLIENT_TIMEOUT_SECOND = 2
    _DEFAULT_ONLINE_STORE_REST_CLIENT_VERIFY_CERTS = True
    _DEFAULT_ONLINE_STORE_REST_CLIENT_USE_SSL = True
    _DEFAULT_ONLINE_STORE_REST_CLIENT_SERVER_API_VERSION = "0.1.0"
    _DEFAULT_ONLINE_STORE_REST_CLIENT_HTTP_AUTHORIZATION = "X-API-KEY"

    def __init__(
        self,
        transport: Optional[
            Union[requests.adapaters.HTTPadapter, requests.adapters.BaseAdapter]
        ] = None,
        optional_config: Optional[Dict[str, Any]] = None,
    ):
        _logger.debug(
            f"Initialising Online Store Rest Client {'with optional configuration' if optional_config else ''}."
        )
        if optional_config:
            _logger.debug(f"Optional Config: {optional_config!r}")
        self._check_hopsworks_connection()
        self.variable_api = variable_api.VariableApi()
        self._auth: client.auth.OnlineStoreKeyAuth
        self._session: requests.Session
        self._current_config: Dict[str, Any]
        self._base_url: furl
        self._setup_rest_client(
            transport=transport,
            optional_config=optional_config,
            use_current_config=False,
        )
        self.is_connected()

    def reset_client(
        self,
        transport: Optional[
            Union[requests.adapters.HttpAdapter, requests.adapters.BaseAdapter]
        ] = None,
        optional_config: Optional[Dict[str, Any]] = None,
    ):
        _logger.debug(
            f"Resetting Online Store Rest Client {'with optional configuration' if optional_config else ''}."
        )
        if optional_config:
            _logger.debug(f"Optional Config: {optional_config}")
        self._check_hopsworks_connection()
        if hasattr(self, "_session") and self._session:
            _logger.debug("Closing existing session.")
            self._session.close()
            delattr(self, "_session")
        self._setup_rest_client(
            transport=transport,
            optional_config=optional_config,
            use_current_config=False if optional_config else True,
        )

    def _setup_rest_client(
        self,
        transport: Optional[
            Union[requests.adapters.HttpAdapter, requests.adapters.BaseAdapter]
        ] = None,
        optional_config: Optional[Dict[str, Any]] = None,
        use_current_config: bool = True,
    ):
        _logger.debug("Setting up Online Store Rest Client.")
        if optional_config and not isinstance(optional_config, dict):
            raise ValueError(
                "optional_config must be a dictionary. See documentation for allowed keys and values."
            )
        _logger.debug("Optional Config: %s", optional_config)
        if not use_current_config:
            _logger.debug(
                "Retrieving default configuration for Online Store REST Client."
            )
            self._current_config = self._get_default_client_config()
        if optional_config:
            _logger.debug(
                "Updating default configuration with provided optional configuration."
            )
            self._current_config.update(optional_config)

        self._set_auth(optional_config)
        if not hasattr(self, "_session") or not self._session:
            _logger.debug("Initialising new requests session.")
            self._session = requests.Session()
        else:
            raise ValueError(
                "Use the init_or_reset_online_store_connection method with reset_connection flag set "
                + "to True to reset the online_store_client_connection"
            )
        if transport is not None:
            _logger.debug("Setting custom transport adapter.")
            self._session.mount("https://", transport)
            self._session.mount("http://", transport)

        if not self._current_config[self.VERIFY_CERTS]:
            _logger.warning(
                "Disabling SSL certificate verification. This is not recommended for production environments."
            )
            self._session.verify = False
        else:
            _logger.debug(
                f"Setting SSL certificate verification using CA Certs path: {self._current_config[self.CA_CERTS]}"
            )
            self._session.verify = self._current_config[self.CA_CERTS]

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

    def _get_default_client_config(self) -> Dict[str, Any]:
        _logger.debug("Retrieving default configuration for Online Store REST Client.")
        default_config = self._get_default_static_parameters_config()
        default_config.update(self._get_default_dynamic_parameters_config())
        return default_config

    def _get_default_static_parameters_config(self) -> Dict[str, Any]:
        _logger.debug(
            "Retrieving default static configuration for Online Store REST Client."
        )
        return {
            self.TIMEOUT: self._DEFAULT_ONLINE_STORE_REST_CLIENT_TIMEOUT_SECOND,
            self.VERIFY_CERTS: self._DEFAULT_ONLINE_STORE_REST_CLIENT_VERIFY_CERTS,
            self.USE_SSL: self._DEFAULT_ONLINE_STORE_REST_CLIENT_USE_SSL,
            self.SERVER_API_VERSION: self._DEFAULT_ONLINE_STORE_REST_CLIENT_SERVER_API_VERSION,
            self.HTTP_AUTHORIZATION: self._DEFAULT_ONLINE_STORE_REST_CLIENT_HTTP_AUTHORIZATION,
        }

    def _get_default_dynamic_parameters_config(
        self,
    ) -> Dict[str, Any]:
        _logger.debug(
            "Retrieving default dynamic configuration for Online Store REST Client."
        )
        url = furl(self._get_rondb_rest_server_endpoint())
        _logger.debug(f"Default RonDB Rest Server host and port: {url.host}:{url.port}")
        _logger.debug(
            f"Using CA Certs from Hopsworks Client: {client.get_instance()._get_ca_chain_path()}"
        )
        return {
            self.HOST: url.host,
            self.PORT: url.port,
            self.CA_CERTS: client.get_instance()._get_ca_chain_path(),
        }

    def _get_rondb_rest_server_endpoint(self) -> str:
        """Retrieve RonDB Rest Server endpoint based on whether the client is running internally or externally.

        If the client is running externally, the endpoint is retrieved via the loadbalancer.
        If the client is running internally, the endpoint is retrieved via (consul) service discovery.
        The default port for the RonDB Rest Server is 4406 and always used unless specifying a different port
        in the configuration.

        Returns:
            str: RonDB Rest Server endpoint with default port.
        """
        if client.get_instance()._is_external():
            _logger.debug(
                "External Online Store REST Client : Retrieving RonDB Rest Server endpoint via loadbalancer."
            )
            external_domain = self.variable_api.get_loadbalancer_external_domain()
            if external_domain == "":
                _logger.debug(
                    "External Online Store REST Client : Loadbalancer external domain is not set. Using client host as endpoint."
                )
                external_domain = client.get_instance().host
            default_url = f"https://{external_domain}:{self._DEFAULT_ONLINE_STORE_REST_CLIENT_PORT}"
            _logger.debug(
                f"External Online Store REST Client : Default RonDB Rest Server endpoint: {default_url}"
            )
            return default_url
        else:
            _logger.debug(
                "Internal Online Store REST Client : Retrieving RonDB Rest Server endpoint via service discovery."
            )
            service_discovery_domain = self.variable_api.get_service_discovery_domain()
            if service_discovery_domain == "":
                raise FeatureStoreException("Service discovery domain is not set.")
            default_url = f"https://rdrs.service.{service_discovery_domain}:{self._DEFAULT_ONLINE_STORE_REST_CLIENT_PORT}"
            _logger.debug(
                f"Internal Online Store REST Client : Default RonDB Rest Server endpoint: {default_url}"
            )
            return default_url

    def send_request(
        self,
        method: str,
        path_params: List[str],
        headers: Optional[Dict[str, Any]] = None,
        data: Optional[str] = None,
    ) -> requests.Response:
        url = self._base_url.copy()
        url.path.segments.extend(path_params)
        _logger.debug(f"Sending {method} request to {url.url}.")
        _logger.debug(f"Provided Data: {data}")
        _logger.debug(f"Provided Headers: {headers}")
        prepped_request = self._session.prepare_request(
            requests.Request(
                method, url=url.url, headers=headers, data=data, auth=self.auth
            )
        )
        timeout = self._current_config[self.TIMEOUT]
        return self._session.send(
            prepped_request,
            # compatibility with 3.7
            timeout=timeout if timeout < 500 else timeout / 1000,
        )

    def _check_hopsworks_connection(self) -> None:
        _logger.debug("Checking Hopsworks connection.")
        assert (
            client.get_instance() is not None and client.get_instance()._connected
        ), """Hopsworks Client is not connected. Please connect to Hopsworks cluster
            via hopsworks.login or hsfs.connection before initialising the Online Store REST Client.
            """
        _logger.debug("Hopsworks connection is active.")

    def _set_auth(self, optional_config: Optional[Dict[str, Any]] = None) -> None:
        """Set authentication object for the Online Store REST Client.

        RonDB Rest Server uses Hopsworks Api Key to authenticate requests via the X-API-KEY header by default.
        The api key determines the permissions of the user making the request for access to a given Feature Store.
        """
        _logger.debug("Setting authentication for Online Store REST Client.")
        if client.get_instance()._is_external():
            assert hasattr(
                client.get_instance()._auth, "_token"
            ), "External client must use API Key authentication. Contact your system administrator."
            _logger.debug(
                "External Online Store REST Client : Setting authentication using Hopsworks Client API Key."
            )
            self._auth = client.auth.OnlineStoreKeyAuth(
                client.get_instance()._auth._token
            )
        elif isinstance(optional_config, dict) and optional_config.get(
            self.API_KEY, False
        ):
            _logger.debug(
                "Setting authentication using provided API Key from optional configuration."
            )
            self._auth = client.auth.OnlineStoreKeyAuth(optional_config[self.API_KEY])
        elif hasattr(self, "_auth") and self._auth is not None:
            _logger.debug(
                "Authentication for Online Store REST Client is already set. Using existing authentication api key."
            )
        else:
            raise FeatureStoreException(
                "RonDB Rest Server uses Hopsworks Api Key to authenticate request."
                + f"Provide a configuration with the {self.API_KEY} key."
            )

    def is_connected(self):
        """If Online Store Rest Client is initialised, ping RonDB Rest Server to ensure connection is active."""
        if self._session is None:
            _logger.debug(
                "Checking Online Store REST Client is connected. Session is not initialised."
            )
            raise FeatureStoreException("Online Store REST Client is not initialised.")

        _logger.debug(
            "Checking Online Store REST Client is connected. Pinging RonDB Rest Server."
        )
        if not self.send_request("GET", ["ping"]):
            warn("Ping failed, RonDB Rest Server is not reachable.", stacklevel=2)
            return False
        return True

    @property
    def session(self) -> requests.Session:
        """Requests session object used to send requests to the Online Store REST API."""
        return self._session

    @property
    def base_url(self) -> furl:
        """Base URL for the Online Store REST API.

        This the url of the RonDB REST Server and should not be confused with the Opensearch Vector DB which also serves as an Online Store for features belonging to Feature Group containing embeddings."""
        return self._base_url

    @property
    def current_config(self) -> Dict[str, Any]:
        """Current configuration of the Online Store REST Client."""
        return self._current_config

    @property
    def auth(self) -> "client.auth.OnlineStoreKeyAuth":
        """Authentication object used to authenticate requests to the Online Store REST API.

        Extends the requests.auth.AuthBase class.
        """
        return self._auth
