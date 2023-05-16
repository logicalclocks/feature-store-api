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
import pyarrow
import pyarrow.flight
from pyarrow.flight import FlightServerError
from hsfs import client
from hsfs import feature_group
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core.variable_api import VariableApi

_arrow_flight_instance = None


def get_instance():
    global _arrow_flight_instance
    if not _arrow_flight_instance:
        _arrow_flight_instance = ArrowFlightClient()
    return _arrow_flight_instance


class ArrowFlightClient:

    SUPPORTED_FORMATS = ["parquet"]
    FILTER_NUMERIC_TYPES = ["bigint", "tinyint", "smallint", "int", "float", "double"]

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
            f"Could not establish connection to FlyingDuck. ({message}) "
            f"Will fall back to hive/spark for this session. "
            f"If the error persists, you can disable FlyingDuck "
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
        if (
            read_options
            and "use_hive" in read_options
            and read_options["use_hive"] is True
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
        supported = (
            isinstance(query._left_feature_group, feature_group.FeatureGroup)
            and query._left_feature_group.time_travel_format == "HUDI"
            and (
                query._left_feature_group_start_time is None
                or query._left_feature_group_start_time == 0
            )
            and query._left_feature_group_end_time is None
        )
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

    def _handle_afs_exception(method):
        def afs_error_handler_wrapper(*args, **kw):
            try:
                return method(*args, **kw)
            except Exception as e:
                message = str(e)
                if (
                    isinstance(e, FlightServerError)
                    and "Please register client certificates first." in message
                ):
                    self = args[0]
                    self._register_certificates()
                    return method(*args, **kw)
                else:
                    raise FeatureStoreException(
                        "Could not read data using FlyingDuck. "
                        "If the issue persists, "
                        'use read_options={"use_hive": True} instead.'
                    ) from e

        return afs_error_handler_wrapper

    def _get_dataset(self, descriptor):
        info = self._connection.get_flight_info(descriptor)
        reader = self._connection.do_get(self._info_to_ticket(info))
        return reader.read_pandas()

    @_handle_afs_exception
    def read_query(self, query_object):
        query_encoded = json.dumps(query_object).encode("ascii")
        descriptor = pyarrow.flight.FlightDescriptor.for_command(query_encoded)
        return self._get_dataset(descriptor)

    @_handle_afs_exception
    def read_path(self, path):
        descriptor = pyarrow.flight.FlightDescriptor.for_path(path)
        return self._get_dataset(descriptor)

    def is_flyingduck_query_object(self, query_obj):
        return isinstance(query_obj, dict) and "query_string" in query_obj

    def create_query_object(self, query, query_str):
        features = {}
        for fg in query.featuregroups:
            fg_name = self._serialize_featuregroup_name(fg)
            features[fg_name] = [feat.name for feat in fg.features]
        filters = self._serialize_filter_expression(query)
        for feature in features:
            features[feature] = list(features[feature])

        query = {
            "query_string": self._translate_to_duckdb(query, query_str),
            "features": features,
            "filters": filters,
        }
        return query

    def _serialize_featuregroup_name(self, fg):
        return f"{fg._get_project_name()}.{fg.name}_{fg.version}"

    def _serialize_filter_expression(self, query):
        if query.filters is None:
            return None
        return self._serialize_logic(query.filters, query)

    def _serialize_logic(self, logic, query):
        return {
            "type": "logic",
            "logic_type": logic._type,
            "left_filter": self._serialize_filter_or_logic(
                logic._left_f, logic._left_l, query
            ),
            "right_filter": self._serialize_filter_or_logic(
                logic._right_f, logic._right_l, query
            ),
        }

    def _serialize_filter_or_logic(self, filter, logic, query):
        if filter:
            return self._serialize_filter(filter, query)
        elif logic:
            return self._serialize_logic(logic, query)
        else:
            return None

    def _serialize_filter(self, filter, query):
        if isinstance(filter._value, datetime.datetime):
            filter_value = filter._value.strftime("%Y-%m-%d %H:%M:%S")
        else:
            filter_value = filter._value

        return {
            "type": "filter",
            "condition": filter._condition,
            "value": filter_value,
            "feature": self._serialize_feature_name(filter._feature, query),
        }

    def _serialize_feature_name(self, feature, query):
        fg = query._get_featuregroup_by_feature(feature)
        fg_name = self._serialize_featuregroup_name(fg)
        return f"{fg_name}.{feature.name}"

    def _translate_to_duckdb(self, query, query_str):
        return query_str.replace(
            f"`{query._left_feature_group.feature_store_name}`.`",
            f"`{query._left_feature_group._get_project_name()}.",
        ).replace("`", '"')

    def _info_to_ticket(self, info):
        return info.endpoints[0].ticket
