#
#   Copyright 2023 Hopsworks AB
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

import base64
import datetime
import json
import logging
import warnings
from functools import wraps
from typing import Any, Dict, Optional, Union

import pyarrow
import pyarrow._flight
import pyarrow.flight
from hsfs import client, feature_group, util
from hsfs.client.exceptions import FeatureStoreException
from hsfs.constructor import query
from hsfs.core.constants import HAS_POLARS, polars_not_installed_message
from hsfs.core.variable_api import VariableApi
from hsfs.storage_connector import StorageConnector
from pyarrow.flight import FlightServerError
from retrying import retry


if HAS_POLARS:
    import polars as pl


_logger = logging.getLogger(__name__)


_arrow_flight_instance = None


def get_instance() -> ArrowFlightClient:
    global _arrow_flight_instance
    if not _arrow_flight_instance:
        _arrow_flight_instance = ArrowFlightClient()
    return _arrow_flight_instance


def close() -> None:
    global _arrow_flight_instance
    _arrow_flight_instance = None


def _disable_feature_query_service_client():
    global _arrow_flight_instance
    _logger.debug("Disabling Hopsworks Feature Query Service Client.")
    if _arrow_flight_instance is None:
        _arrow_flight_instance.ArrowFlightClient(disabled_for_session=True)
    else:
        _arrow_flight_instance._disable_for_session(on_purpose=True)


def _should_retry(exception):
    return isinstance(exception, pyarrow._flight.FlightUnavailableError)


def _is_feature_query_service_queue_full_error(exception):
    return isinstance(
        exception, pyarrow._flight.FlightServerError
    ) and "no free slot available for" in str(exception)


def _is_no_commits_found_error(exception):
    return isinstance(
        exception, pyarrow._flight.FlightServerError
    ) and "No commits found" in str(exception)


def _should_retry_healthcheck_or_certificate_registration(exception):
    return (
        isinstance(exception, pyarrow._flight.FlightUnavailableError)
        or isinstance(exception, pyarrow._flight.FlightTimedOutError)
        # not applicable for healthcheck, only certificate registration
        or _is_feature_query_service_queue_full_error(exception)
    )


# Avoid unnecessary client init
def is_data_format_supported(data_format: str, read_options: Optional[Dict[str, Any]]):
    if data_format not in ArrowFlightClient.SUPPORTED_FORMATS:
        return False
    elif read_options and (
        read_options.get("use_hive", False) or read_options.get("use_spark", False)
    ):
        return False
    else:
        return get_instance()._should_be_used()


def _is_query_supported_rec(query: query.Query):
    hudi_no_time_travel = (
        isinstance(query._left_feature_group, feature_group.FeatureGroup)
        and query._left_feature_group.time_travel_format == "HUDI"
        and (
            query._left_feature_group_start_time is None
            or query._left_feature_group_start_time == 0
        )
        and query._left_feature_group_end_time is None
    )
    supported_connector = (
        isinstance(query._left_feature_group, feature_group.ExternalFeatureGroup)
        and query._left_feature_group.storage_connector.type
        in ArrowFlightClient.SUPPORTED_EXTERNAL_CONNECTORS
    )
    delta_s3 = (
        isinstance(query._left_feature_group, feature_group.FeatureGroup)
        and query._left_feature_group.time_travel_format == "DELTA"
        and query._left_feature_group.storage_connector
        and query._left_feature_group.storage_connector.type == StorageConnector.S3
    )
    supported = hudi_no_time_travel or supported_connector or delta_s3
    for j in query._joins:
        supported &= _is_query_supported_rec(j._query)
    return supported


def is_query_supported(query: query.Query, read_options: Optional[Dict[str, Any]]):
    if read_options and (
        read_options.get("use_hive", False) or read_options.get("use_spark", False)
    ):
        return False
    elif not _is_query_supported_rec(query):
        return False
    else:
        return get_instance()._should_be_used()


class ArrowFlightClient:
    SUPPORTED_FORMATS = ["parquet"]
    SUPPORTED_EXTERNAL_CONNECTORS = [
        StorageConnector.SNOWFLAKE,
        StorageConnector.BIGQUERY,
    ]
    READ_ERROR = 'Could not read data using Hopsworks Feature Query Service. If the issue persists, use read_options={"use_hive": True} instead.'
    WRITE_ERROR = 'Could not write data using Hopsworks Feature Query Service. If the issue persists, use write_options={"use_spark": True} instead.'
    DEFAULTING_TO_DIFFERENT_SERVICE_WARNING = (
        "Defaulting to Hive/Spark execution for this call."
    )
    CLIENT_WILL_STAY_ACTIVE_WARNING = 'The client will remain active for future calls. If the issue persists, use read_options={"use_hive": True} or write_options={"use_spark": True}.'
    DEFAULT_TIMEOUT_SECONDS = 900
    DEFAULT_HEALTHCHECK_TIMEOUT_SECONDS = 5
    DEFAULT_GRPC_MIN_RECONNECT_BACKOFF_MS = 2000

    def __init__(self, disabled_for_session: bool = False):
        _logger.debug("Initializing Hopsworks Feature Query Service Client.")
        self._timeout: float = ArrowFlightClient.DEFAULT_TIMEOUT_SECONDS
        self._health_check_timeout: float = (
            ArrowFlightClient.DEFAULT_HEALTHCHECK_TIMEOUT_SECONDS
        )

        self._enabled_on_cluster: bool = False
        self._host_url: Optional[str] = None
        self._connection: Optional[pyarrow.flight.FlightClient] = None
        if disabled_for_session:
            self._disable_for_session(on_purpose=True)
            return
        else:
            self._disabled_for_session: bool = False

        self._client = client.get_instance()
        self._variable_api: VariableApi = VariableApi()

        try:
            self._check_cluster_service_enabled()
            self._host_url = self._retrieve_host_url()

            if self._enabled_on_cluster:
                _logger.debug(
                    "Hopsworks Feature Query Service is enabled on the cluster."
                )
                self._initialize_flight_client()
            else:
                _logger.debug(
                    "Hopsworks Feature Query Service Client is not enabled on the cluster or a cluster variable is misconfigured."
                )
                self._disable_for_session()
                return
        except Exception as e:
            _logger.debug("Failed to connect to Hopsworks Feature Query Service")
            _logger.exception(e)
            self._disable_for_session(str(e))
            return

        try:
            self._health_check()
            self._register_certificates()
        except Exception as e:
            _logger.debug("Failed to connect to Hopsworks Feature Query Service")
            _logger.exception(e)
            warnings.warn(
                f"Failed to connect to Hopsworks Feature Query Service, got {str(e)}."
                + ArrowFlightClient.DEFAULTING_TO_DIFFERENT_SERVICE_WARNING
                + ArrowFlightClient.CLIENT_WILL_STAY_ACTIVE_WARNING,
                stacklevel=1,
            )
            return

    def _check_cluster_service_enabled(self) -> None:
        try:
            _logger.debug(
                "Connecting to Hopsworks Cluster to check if Feature Query Service is enabled."
            )
            self._enabled_on_cluster = self._variable_api.get_flyingduck_enabled()
        except Exception as e:
            # if feature flag cannot be retrieved, assume it is disabled
            _logger.debug(
                "Unable to fetch Hopsworks Feature Query Service flag, disabling HFQS client."
            )
            _logger.exception(e)
            self._enabled_on_cluster = False

    def _retrieve_host_url(self) -> Optional[str]:
        _logger.debug("Retrieving host URL.")
        if isinstance(self._client, client.external.Client):
            external_domain = self._variable_api.get_loadbalancer_external_domain()
            if external_domain == "":
                _logger.debug("loadbalancer_external_domain not set on cluster")
                return None
            host_url = f"grpc+tls://{external_domain}:5005"
        else:
            service_discovery_domain = self._variable_api.get_service_discovery_domain()
            if service_discovery_domain == "":
                _logger.debug("service_discovery_domain not set on cluster")
                return None
            host_url = f"grpc+tls://flyingduck.service.{service_discovery_domain}:5005"
        _logger.debug(
            f"Connecting to Hopsworks Feature Query Service on host {host_url}"
        )
        return host_url

    def _disable_for_session(
        self, message: Optional[str] = None, on_purpose: bool = False
    ) -> None:
        self._disabled_for_session = True
        if on_purpose:
            warnings.warn(
                "Hopsworks Feature Query Service will be disabled for this session.",
                stacklevel=1,
            )
        if self._enabled_on_cluster:
            warnings.warn(
                "Hospworks Feature Query Service is disabled on cluster. Contact your administrator to enable it.",
                stacklevel=1,
            )
        else:
            warnings.warn(
                f"Client initialisation failed: {message}. Hopsworks Feature Query Service will be disabled for this session."
                "If you believe this is a transient error, you can call `(hopsworks.)hsfs.reset_offline_query_service_client()`"
                " to re-enable it.",
                stacklevel=1,
            )

    def _initialize_flight_client(self):
        (tls_root_certs, cert_chain, private_key) = self._extract_certs()
        self._connection = pyarrow.flight.FlightClient(
            location=self.host_url,
            tls_root_certs=tls_root_certs,
            cert_chain=cert_chain,
            private_key=private_key,
            override_hostname="flyingduck.service.consul",
            generic_options=[
                (
                    # https://arrow.apache.org/docs/cpp/flight.html#excessive-traffic
                    "GRPC_ARG_MIN_RECONNECT_BACKOFF_MS",
                    ArrowFlightClient.DEFAULT_GRPC_MIN_RECONNECT_BACKOFF_MS,
                )
            ],
        )

    @retry(
        wait_exponential_multiplier=1000,
        stop_max_attempt_number=5,
        retry_on_exception=_should_retry_healthcheck_or_certificate_registration,
    )
    def _health_check(self):
        _logger.debug("Performing healthcheck of Hopsworks Feature Query Service.")
        action = pyarrow.flight.Action("healthcheck", b"")
        options = pyarrow.flight.FlightCallOptions(timeout=self.health_check_timeout)
        list(self._connection.do_action(action, options=options))
        _logger.debug("Healthcheck succeeded.")

    def _should_be_used(self):
        if not self._enabled_on_cluster:
            _logger.debug(
                "Hopsworks Feature Query Service not used as it is disabled on the cluster."
            )
            return False

        if self._disabled_for_session:
            _logger.debug(
                "Hopsworks Feature Query Service client failed to initialise and is disabled for the session."
            )
            return False

        _logger.debug("Using Hopsworks Feature Query Service.")
        return True

    def _extract_certs(self):
        _logger.debug("Extracting client certificates.")
        with open(self._client._get_ca_chain_path(), "rb") as f:
            tls_root_certs = f.read()
        with open(self._client._get_client_cert_path(), "r") as f:
            cert_chain = f.read()
        with open(self._client._get_client_key_path(), "r") as f:
            private_key = f.read()
        return tls_root_certs, cert_chain, private_key

    def _encode_certs(self, path):
        _logger.debug(f"Encoding certificates from path: {path}")
        with open(path, "rb") as f:
            content = f.read()
            return base64.b64encode(content).decode("utf-8")

    @retry(
        wait_exponential_multiplier=1000,
        stop_max_attempt_number=3,
        retry_on_exception=_should_retry_healthcheck_or_certificate_registration,
    )
    def _register_certificates(self):
        kstore = self._encode_certs(self._client._get_jks_key_store_path())
        tstore = self._encode_certs(self._client._get_jks_trust_store_path())
        cert_key = self._client._cert_key
        certificates_json = json.dumps(
            {"kstore": kstore, "tstore": tstore, "cert_key": cert_key}
        ).encode("ascii")
        certificates_json_buf = pyarrow.py_buffer(certificates_json)
        action = pyarrow.flight.Action(
            "register-client-certificates", certificates_json_buf
        )
        # Registering certificates queue time occasionally spike.
        options = pyarrow.flight.FlightCallOptions(timeout=self.health_check_timeout)
        _logger.debug(
            "Registering client certificates with Hopsworks Feature Query Service."
        )
        self._connection.do_action(action, options=options)
        _logger.debug("Client certificates registered.")

    def _handle_afs_exception(user_message="None"):
        def decorator(func):
            @wraps(func)
            def afs_error_handler_wrapper(instance, *args, **kw):
                try:
                    return func(instance, *args, **kw)
                except Exception as e:
                    message = str(e)
                    _logger.debug("Caught exception in %s: %s", func.__name__, message)
                    _logger.exception(e)
                    if (
                        isinstance(e, FlightServerError)
                        and "Please register client certificates first." in message
                    ):
                        instance._register_certificates()
                        return func(instance, *args, **kw)
                    elif _is_feature_query_service_queue_full_error(e):
                        raise FeatureStoreException(
                            "Hopsworks Feature Query Service is busy right now. Please try again later."
                        ) from e
                    elif _is_no_commits_found_error(e):
                        raise FeatureStoreException(str(e).split("Details:")[0]) from e
                    else:
                        raise FeatureStoreException(user_message) from e

            return afs_error_handler_wrapper

        return decorator

    @retry(
        wait_exponential_multiplier=1000,
        stop_max_attempt_number=3,
        retry_on_exception=_should_retry,
    )
    def get_flight_info(self, descriptor):
        # The timeout needs not be as long as timeout for do_get or do_action
        _logger.debug("Getting flight info for descriptor: %s", str(descriptor))
        options = pyarrow.flight.FlightCallOptions(timeout=self.health_check_timeout)
        return self._connection.get_flight_info(
            descriptor,
            options=options,
        )

    @retry(
        wait_exponential_multiplier=1000,
        stop_max_attempt_number=3,
        retry_on_exception=_should_retry,
    )
    def _get_dataset(self, descriptor, timeout=None, dataframe_type="pandas"):
        if timeout is None:
            timeout = self.timeout
        info = self.get_flight_info(descriptor)
        _logger.debug("Retrieved flight info: %s. Fetching dataset.", str(info))
        options = pyarrow.flight.FlightCallOptions(timeout=timeout)
        reader = self._connection.do_get(info.endpoints[0].ticket, options)
        _logger.debug("Dataset fetched. Converting to dataframe %s.", dataframe_type)
        if dataframe_type.lower() == "polars":
            if not HAS_POLARS:
                raise ModuleNotFoundError(polars_not_installed_message)
            return pl.from_arrow(reader.read_all())
        else:
            return reader.read_pandas()

    # retry is handled in get_dataset
    @_handle_afs_exception(user_message=READ_ERROR)
    def read_query(self, query_object, arrow_flight_config, dataframe_type):
        query_encoded = json.dumps(query_object).encode("ascii")
        descriptor = pyarrow.flight.FlightDescriptor.for_command(query_encoded)
        return self._get_dataset(
            descriptor,
            (
                arrow_flight_config.get("timeout", self.timeout)
                if arrow_flight_config
                else self.timeout
            ),
            dataframe_type,
        )

    # retry is handled in get_dataset
    @_handle_afs_exception(user_message=READ_ERROR)
    def read_path(self, path, arrow_flight_config, dataframe_type):
        descriptor = pyarrow.flight.FlightDescriptor.for_path(path)
        return self._get_dataset(
            descriptor,
            timeout=arrow_flight_config.get("timeout", self.timeout)
            if arrow_flight_config
            else self.timeout,
            dataframe_type=dataframe_type,
        )

    @_handle_afs_exception(user_message=WRITE_ERROR)
    @retry(
        wait_exponential_multiplier=1000,
        stop_max_attempt_number=3,
        # this retry only on FlightUnavailableError, which is only raised as long as
        # do_action call did not trigger any write on the server
        # Technically same as _should_retry, used for read operations but to avoid future mistakes
        # it is better to have separate retry_on_exception function with explicit limitations
        retry_on_exception=lambda e: isinstance(
            e, pyarrow._flight.FlightUnavailableError
        ),
    )
    def create_training_dataset(
        self, feature_view_obj, training_dataset_obj, query_obj, arrow_flight_config
    ):
        training_dataset = {}
        training_dataset["fs_name"] = util.strip_feature_store_suffix(
            training_dataset_obj.feature_store_name
        )
        training_dataset["fv_name"] = feature_view_obj.name
        training_dataset["fv_version"] = feature_view_obj.version
        training_dataset["tds_version"] = training_dataset_obj.version
        training_dataset["query"] = query_obj
        _logger.debug(f"Creating training dataset: {training_dataset}")
        try:
            training_dataset_encoded = json.dumps(training_dataset).encode("ascii")
            training_dataset_buf = pyarrow.py_buffer(training_dataset_encoded)
            action = pyarrow.flight.Action(
                "create-training-dataset", training_dataset_buf
            )
            timeout = (
                arrow_flight_config.get("timeout", self.timeout)
                if arrow_flight_config
                else self.timeout
            )
            options = pyarrow.flight.FlightCallOptions(timeout=timeout)
            for result in self._connection.do_action(action, options):
                return result.body.to_pybytes()
        except pyarrow.lib.ArrowIOError as e:
            _logger.debug("Caught ArrowIOError in create_training_dataset: %s", str(e))
            _logger.exception(e)
            print("Error calling action:", e)

    def create_query_object(self, query, query_str, on_demand_fg_aliases=None):
        if on_demand_fg_aliases is None:
            on_demand_fg_aliases = []
        features = {}
        connectors = {}
        for fg in query.featuregroups:
            fg_name = _serialize_featuregroup_name(fg)
            fg_connector = _serialize_featuregroup_connector(
                fg, query, on_demand_fg_aliases
            )
            features[fg_name] = [feat.name for feat in fg.features]
            connectors[fg_name] = fg_connector
        filters = _serialize_filter_expression(query.filters, query)

        query = {
            "query_string": _translate_to_duckdb(query, query_str),
            "features": features,
            "filters": filters,
            "connectors": connectors,
        }
        return query

    def is_enabled(self):
        if self._disabled_for_session or not self._enabled_on_cluster:
            return False
        return True

    @property
    def timeout(self) -> Union[int, float]:
        """Timeout in seconds for Hopsworks Feature Query Service do_get or do_action operations, not including the healthcheck."""
        return self._timeout

    @timeout.setter
    def timeout(self, value: Union[int, float]) -> None:
        self._timeout = value

    @property
    def health_check_timeout(self) -> Union[int, float]:
        """Timeout in seconds for the healthcheck operation."""
        return self._health_check_timeout

    @health_check_timeout.setter
    def health_check_timeout(self, value: Union[int, float]) -> None:
        self._health_check_timeout = value

    @property
    def host_url(self) -> Optional[str]:
        """URL of Hopsworks Feature Query Service."""
        return self._host_url

    @host_url.setter
    def host_url(self, value: str) -> None:
        self._host_url = value

    @property
    def disabled_for_session(self) -> bool:
        """Whether the client is disabled for the current session."""
        return self._disabled_for_session

    @property
    def enabled_on_cluster(self) -> bool:
        """Whether the client is enabled on the cluster."""
        return self._enabled_on_cluster


def _serialize_featuregroup_connector(fg, query, on_demand_fg_aliases):
    connector = {}
    if isinstance(fg, feature_group.ExternalFeatureGroup):
        connector["time_travel_type"] = None
        connector["type"] = fg.storage_connector.type
        connector["options"] = fg.storage_connector.connector_options()
        connector["query"] = fg.query[:-1] if fg.query.endswith(";") else fg.query
        for on_demand_fg_alias in on_demand_fg_aliases:
            if on_demand_fg_alias.on_demand_feature_group.name == fg.name:
                connector["alias"] = on_demand_fg_alias.alias
                break
        if query._left_feature_group == fg:
            connector["filters"] = _serialize_filter_expression(
                query._filter, query, True
            )
        else:
            for join_obj in query._joins:
                if join_obj._query._left_feature_group == fg:
                    connector["filters"] = _serialize_filter_expression(
                        join_obj._query._filter, join_obj._query, True
                    )
    elif fg.time_travel_format == "DELTA":
        connector["time_travel_type"] = "delta"
        connector["type"] = fg.storage_connector.type
        connector["options"] = fg.storage_connector.connector_options()
        if fg.storage_connector.type == StorageConnector.S3:
            connector["options"]["path"] = fg.location
        connector["query"] = ""
        if query._left_feature_group == fg:
            connector["filters"] = _serialize_filter_expression(
                query._filter, query, True
            )
        else:
            for join_obj in query._joins:
                if join_obj._query._left_feature_group == fg:
                    connector["filters"] = _serialize_filter_expression(
                        join_obj._query._filter, join_obj._query, True
                    )
    else:
        connector["time_travel_type"] = "hudi"
    return connector


def _serialize_featuregroup_name(fg):
    return f"{fg._get_project_name()}.{fg.name}_{fg.version}"


def _serialize_filter_expression(filters, query, short_name=False):
    if filters is None:
        return None
    return _serialize_logic(filters, query, short_name)


def _serialize_logic(logic, query, short_name):
    return {
        "type": "logic",
        "logic_type": logic._type,
        "left_filter": _serialize_filter_or_logic(
            logic._left_f, logic._left_l, query, short_name
        ),
        "right_filter": _serialize_filter_or_logic(
            logic._right_f, logic._right_l, query, short_name
        ),
    }


def _serialize_filter_or_logic(filter, logic, query, short_name):
    if filter:
        return _serialize_filter(filter, query, short_name)
    elif logic:
        return _serialize_logic(logic, query, short_name)
    else:
        return None


def _serialize_filter(filter, query, short_name):
    if isinstance(filter._value, datetime.datetime):
        filter_value = filter._value.strftime("%Y-%m-%d %H:%M:%S")
    else:
        filter_value = filter._value

    return {
        "type": "filter",
        "condition": filter._condition,
        "value": filter_value,
        "feature": _serialize_feature_name(filter._feature, query, short_name),
    }


def _serialize_feature_name(feature, query, short_name):
    if short_name:
        return feature.name
    fg = query._get_featuregroup_by_feature(feature)
    fg_name = _serialize_featuregroup_name(fg)
    return f"{fg_name}.{feature.name}"


def _translate_to_duckdb(query, query_str):
    translated = query_str
    for fg in query.featuregroups:
        translated = translated.replace(
            f"`{fg.feature_store_name}`.`",
            f"`{fg._get_project_name()}.",
        )
    return translated.replace("`", '"')


def supports(featuregroups):
    if len(featuregroups) > sum(
        1
        for fg in featuregroups
        if isinstance(
            fg,
            (feature_group.FeatureGroup, feature_group.ExternalFeatureGroup),
        )
    ):
        # Contains unsupported feature group types such as a spine group
        return False

    for fg in filter(
        lambda fg: isinstance(fg, feature_group.ExternalFeatureGroup), featuregroups
    ):
        if (
            fg.storage_connector.type
            not in ArrowFlightClient.SUPPORTED_EXTERNAL_CONNECTORS
        ):
            return False
    return True
