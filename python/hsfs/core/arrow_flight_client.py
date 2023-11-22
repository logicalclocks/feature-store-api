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

import datetime
import json
import base64
import warnings
from functools import wraps

import pyarrow
import pyarrow.flight
from pyarrow.flight import FlightServerError
from hsfs import client
from hsfs import feature_group
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core.variable_api import VariableApi
from hsfs import util
from hsfs.storage_connector import StorageConnector
from retrying import retry

_arrow_flight_instance = None


def get_instance():
    global _arrow_flight_instance
    if not _arrow_flight_instance:
        _arrow_flight_instance = ArrowFlightClient()
    return _arrow_flight_instance


def close():
    global _arrow_flight_instance
    _arrow_flight_instance = None


class ArrowFlightClient:
    SUPPORTED_FORMATS = ["parquet"]
    SUPPORTED_EXTERNAL_CONNECTORS = [
        StorageConnector.SNOWFLAKE,
        StorageConnector.BIGQUERY,
    ]
    FILTER_NUMERIC_TYPES = ["bigint", "tinyint", "smallint", "int", "float", "double"]
    READ_ERROR = 'Could not read data using ArrowFlight. If the issue persists, use read_options={"use_hive": True} instead.'
    WRITE_ERROR = 'Could not write data using ArrowFlight. If the issue persists, use write_options={"use_spark": True} instead.'
    DEFAULT_TIMEOUT = 900

    def __init__(self):
        try:
            self._variable_api = VariableApi()
            self._is_enabled = self._variable_api.get_flyingduck_enabled()
        except Exception:
            # if feature flag cannot be retrieved, assume it is disabled
            self._is_enabled = False
            return

        if self._is_enabled:
            try:
                self._initialize_connection()
            except Exception as e:
                self._disable(str(e))

    def _disable(self, message):
        self._is_enabled = False
        warnings.warn(
            f"Could not establish connection to ArrowFlight Server. ({message}) "
            f"Will fall back to hive/spark for this session. "
            f"If the error persists, you can disable using ArrowFlight "
            f"by changing the cluster configuration (set 'enable_flyingduck'='false')."
        )

    def _initialize_connection(self):
        self._client = client.get_instance()

        if isinstance(self._client, client.external.Client):
            external_domain = self._variable_api.get_loadbalancer_external_domain()
            if external_domain == "":
                raise Exception(
                    "External client could not locate loadbalancer_external_domain "
                    "in cluster configuration or variable is empty."
                )
            host_url = f"grpc+tls://{external_domain}:5005"
        else:
            service_discovery_domain = self._variable_api.get_service_discovery_domain()
            if service_discovery_domain == "":
                raise Exception(
                    "Client could not locate service_discovery_domain "
                    "in cluster configuration or variable is empty."
                )
            host_url = f"grpc+tls://flyingduck.service.{service_discovery_domain}:5005"

        (tls_root_certs, cert_chain, private_key) = self._extract_certs(self._client)
        self._connection = pyarrow.flight.FlightClient(
            location=host_url,
            tls_root_certs=tls_root_certs,
            cert_chain=cert_chain,
            private_key=private_key,
            override_hostname="flyingduck.service.consul",
        )
        self._health_check()
        self._register_certificates()

    def _health_check(self):
        action = pyarrow.flight.Action("healthcheck", b"")
        options = pyarrow.flight.FlightCallOptions(timeout=1)
        list(self._connection.do_action(action, options=options))

    def _should_be_used(self, read_options):
        if read_options and (
            read_options.get("use_hive", False) or read_options.get("use_spark", False)
        ):
            return False

        return self._is_enabled

    def is_query_supported(self, query, read_options):
        return self._should_be_used(read_options) and self._is_query_supported_rec(
            query
        )

    def is_data_format_supported(self, data_format, read_options):
        return (
            self._should_be_used(read_options)
            and data_format in ArrowFlightClient.SUPPORTED_FORMATS
        )

    def _is_query_supported_rec(self, query):
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
        supported = hudi_no_time_travel or supported_connector
        for j in query._joins:
            supported &= self._is_query_supported_rec(j._query)
        return supported

    def _extract_certs(self, client):
        with open(client._get_ca_chain_path(), "rb") as f:
            tls_root_certs = f.read()
        with open(client._get_client_cert_path(), "r") as f:
            cert_chain = f.read()
        with open(client._get_client_key_path(), "r") as f:
            private_key = f.read()
        return tls_root_certs, cert_chain, private_key

    def _encode_certs(self, path):
        with open(path, "rb") as f:
            content = f.read()
            return base64.b64encode(content).decode("utf-8")

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
        self._connection.do_action(action)

    def _handle_afs_exception(user_message="None"):
        def decorator(func):
            @wraps(func)
            def afs_error_handler_wrapper(instance, *args, **kw):
                try:
                    return func(instance, *args, **kw)
                except Exception as e:
                    message = str(e)
                    if (
                        isinstance(e, FlightServerError)
                        and "Please register client certificates first." in message
                    ):
                        instance._register_certificates()
                        return func(instance, *args, **kw)
                    else:
                        raise FeatureStoreException(user_message) from e

            return afs_error_handler_wrapper

        return decorator

    @staticmethod
    def _should_retry(exception):
        return isinstance(exception, pyarrow._flight.FlightUnavailableError)

    @retry(
        wait_exponential_multiplier=1000,
        stop_max_attempt_number=5,
        retry_on_exception=_should_retry,
    )
    def get_flight_info(self, descriptor):
        return self._connection.get_flight_info(descriptor)

    def _get_dataset(self, descriptor, timeout=DEFAULT_TIMEOUT):
        info = self.get_flight_info(descriptor)
        options = pyarrow.flight.FlightCallOptions(timeout=timeout)
        reader = self._connection.do_get(self._info_to_ticket(info), options)
        return reader.read_pandas()

    @_handle_afs_exception(user_message=READ_ERROR)
    def read_query(self, query_object, arrow_flight_config):
        query_encoded = json.dumps(query_object).encode("ascii")
        descriptor = pyarrow.flight.FlightDescriptor.for_command(query_encoded)
        return self._get_dataset(
            descriptor,
            arrow_flight_config.get("timeout")
            if arrow_flight_config
            else self.DEFAULT_TIMEOUT,
        )

    @_handle_afs_exception(user_message=READ_ERROR)
    def read_path(self, path, arrow_flight_config):
        descriptor = pyarrow.flight.FlightDescriptor.for_path(path)
        return self._get_dataset(descriptor, arrow_flight_config)

    @_handle_afs_exception(user_message=WRITE_ERROR)
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

        try:
            training_dataset_encoded = json.dumps(training_dataset).encode("ascii")
            training_dataset_buf = pyarrow.py_buffer(training_dataset_encoded)
            action = pyarrow.flight.Action(
                "create-training-dataset", training_dataset_buf
            )
            timeout = (
                arrow_flight_config.get("timeout", self.DEFAULT_TIMEOUT)
                if arrow_flight_config
                else self.DEFAULT_TIMEOUT
            )
            options = pyarrow.flight.FlightCallOptions(timeout=timeout)
            for result in self._connection.do_action(action, options):
                return result.body.to_pybytes()
        except pyarrow.lib.ArrowIOError as e:
            print("Error calling action:", e)

    def is_flyingduck_query_object(self, query_obj):
        return isinstance(query_obj, dict) and "query_string" in query_obj

    def create_query_object(self, query, query_str, on_demand_fg_aliases=[]):
        features = {}
        connectors = {}
        for fg in query.featuregroups:
            fg_name = self._serialize_featuregroup_name(fg)
            fg_connector = self._serialize_featuregroup_connector(
                fg, query, on_demand_fg_aliases
            )
            features[fg_name] = [feat.name for feat in fg.features]
            connectors[fg_name] = fg_connector
        filters = self._serialize_filter_expression(query.filters, query)

        query = {
            "query_string": self._translate_to_duckdb(query, query_str),
            "features": features,
            "filters": filters,
            "connectors": connectors,
        }
        return query

    def _serialize_featuregroup_connector(self, fg, query, on_demand_fg_aliases):
        connector = {}
        if isinstance(fg, feature_group.ExternalFeatureGroup):
            connector["type"] = fg.storage_connector.type
            connector["options"] = fg.storage_connector.connector_options()
            connector["query"] = fg.query[:-1] if fg.query.endswith(";") else fg.query
            for on_demand_fg_alias in on_demand_fg_aliases:
                if on_demand_fg_alias.on_demand_feature_group.name == fg.name:
                    connector["alias"] = on_demand_fg_alias.alias
                    break
            if query._left_feature_group == fg:
                connector["filters"] = self._serialize_filter_expression(
                    query._filter, query, True
                )
            else:
                for join_obj in query._joins:
                    if join_obj._query._left_feature_group == fg:
                        connector["filters"] = self._serialize_filter_expression(
                            join_obj._query._filter, join_obj._query, True
                        )
        else:
            connector["type"] = "hudi"

        return connector

    def _serialize_featuregroup_name(self, fg):
        return f"{fg._get_project_name()}.{fg.name}_{fg.version}"

    def _serialize_filter_expression(self, filters, query, short_name=False):
        if filters is None:
            return None
        return self._serialize_logic(filters, query, short_name)

    def _serialize_logic(self, logic, query, short_name):
        return {
            "type": "logic",
            "logic_type": logic._type,
            "left_filter": self._serialize_filter_or_logic(
                logic._left_f, logic._left_l, query, short_name
            ),
            "right_filter": self._serialize_filter_or_logic(
                logic._right_f, logic._right_l, query, short_name
            ),
        }

    def _serialize_filter_or_logic(self, filter, logic, query, short_name):
        if filter:
            return self._serialize_filter(filter, query, short_name)
        elif logic:
            return self._serialize_logic(logic, query, short_name)
        else:
            return None

    def _serialize_filter(self, filter, query, short_name):
        if isinstance(filter._value, datetime.datetime):
            filter_value = filter._value.strftime("%Y-%m-%d %H:%M:%S")
        else:
            filter_value = filter._value

        return {
            "type": "filter",
            "condition": filter._condition,
            "value": filter_value,
            "feature": self._serialize_feature_name(filter._feature, query, short_name),
        }

    def _serialize_feature_name(self, feature, query, short_name):
        if short_name:
            return feature.name

        fg = query._get_featuregroup_by_feature(feature)
        fg_name = self._serialize_featuregroup_name(fg)
        return f"{fg_name}.{feature.name}"

    def _translate_to_duckdb(self, query, query_str):
        translated = query_str
        for fg in query.featuregroups:
            translated = translated.replace(
                f"`{fg.feature_store_name}`.`",
                f"`{fg._get_project_name()}.",
            )
        return translated.replace("`", '"')

    def _info_to_ticket(self, info):
        return info.endpoints[0].ticket

    def is_enabled(self):
        return self._is_enabled

    def supports(self, featuregroups):
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
            if fg.storage_connector.type not in self.SUPPORTED_EXTERNAL_CONNECTORS:
                return False
        return True
