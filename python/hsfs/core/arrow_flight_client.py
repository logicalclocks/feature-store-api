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
                self._is_enabled = False
                warnings.warn(
                    f"Could not establish connection to FlyingDuck. ({e}) "
                    f"Will fall back to spark for this session. "
                    f"If the error persists, you can disable FlyingDuck "
                    f"by changing the cluster configuration (set 'enable_flyingduck'='false')."
                )

    def _initialize_connection(self):
        self._client = client.get_instance()

        host_url = "grpc+tls://flyingduck.service.consul:5005"
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

    def should_be_used(self, read_options):
        if "use_spark" in read_options and read_options["use_spark"] is True:
            return False

        if self._is_enabled:
            return True

    def is_query_supported(self, query, read_options):
        supported = self._is_query_supported_rec(query)
        return supported and self.should_be_used(read_options)

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

    def is_data_format_supported(self, data_format, read_options):
        supported = data_format in ArrowFlightClient.SUPPORTED_FORMATS
        return supported and self.should_be_used(read_options)

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
                        'use read_options={"use_spark": True} instead.'
                    ) from e

        return afs_error_handler_wrapper

    def _get_dataset(self, descriptor):
        info = self._connection.get_flight_info(descriptor)
        reader = self._connection.do_get(self._info_to_ticket(info))
        return reader.read_pandas()

    @_handle_afs_exception
    def read_query(self, query, query_str):
        query_encoded = json.dumps(self._get_query_object(query, query_str)).encode(
            "ascii"
        )
        descriptor = pyarrow.flight.FlightDescriptor.for_command(query_encoded)
        return self._get_dataset(descriptor)

    @_handle_afs_exception
    def read_path(self, path):
        descriptor = pyarrow.flight.FlightDescriptor.for_path(path)
        return self._get_dataset(descriptor)

    @_handle_afs_exception
    def create_training_dataset(self, feature_view, tds_version=1):
        if not self._is_enabled:
            raise FeatureStoreException("Arrow Flight Service is not enabled.")
        training_dataset_metadata = self._training_dataset_metadata_from_feature_view(
            feature_view, tds_version
        )
        training_dataset_encoded = json.dumps(training_dataset_metadata).encode("ascii")
        buf = pyarrow.py_buffer(training_dataset_encoded)
        action = pyarrow.flight.Action("create-training-dataset", buf)
        for result in self._connection.do_action(action):
            return result.body.to_pybytes()

    def _path_from_feature_view(self, feature_view, tds_version):
        training_dataset_metadata = self._training_dataset_metadata_from_feature_view(
            feature_view, tds_version
        )
        path = (
            f"{training_dataset_metadata['featurestore_name']}_Training_Datasets/"
            f"{training_dataset_metadata['name']}_{training_dataset_metadata['version']}"
            f"_{training_dataset_metadata['tds_version']}/"
            f"{training_dataset_metadata['name']}_{training_dataset_metadata['version']}/"
            f"{training_dataset_metadata['name']}_{training_dataset_metadata['version']}.parquet"
        )
        full_path = f"/Projects/{training_dataset_metadata['featurestore_name']}/{path}"
        return full_path

    def _training_dataset_metadata_from_feature_view(self, feature_view, tds_version):
        training_dataset_metadata = {}
        training_dataset_metadata["name"] = feature_view.name
        training_dataset_metadata["version"] = f"{feature_view.version}"
        training_dataset_metadata["tds_version"] = f"{tds_version}"
        training_dataset_metadata["query"] = self._get_query_object(
            feature_view.query, feature_view.query.to_string()
        )
        training_dataset_metadata[
            "featurestore_name"
        ] = feature_view.query._left_feature_group.feature_store_name.replace(
            "_featurestore", ""
        )

        return training_dataset_metadata

    def _get_query_object(self, query, query_str):
        query_string = query_str.replace(
            f"`{query._left_feature_group.feature_store_name}`.`",
            f"`{query._left_feature_group.feature_store_name.replace('_featurestore','')}.",
        ).replace("`", '"')

        (
            featuregroups,
            features,
            filters,
        ) = self._collect_featuregroups_features_and_filters(query)

        query = {
            "query_string": query_string,
            "featuregroups": featuregroups,
            "features": features,
            "filters": filters,
        }
        return query

    def _update_features(self, features, fg_name, new_features):
        updated_features = features.get(fg_name, set())
        updated_features.update(new_features)
        features[fg_name] = updated_features

    def _collect_featuregroups_features_and_filters(self, query):
        (
            featuregroups,
            features,
            filters,
        ) = self._collect_featuregroups_features_and_filters_rec(query)
        filters = self._filter_to_expression(filters, featuregroups)
        for feature in features:
            features[feature] = list(features[feature])
        return featuregroups, features, filters

    def _collect_featuregroups_features_and_filters_rec(self, query):
        featuregroups = {}
        fg = query._left_feature_group
        fg_name = f"{fg.feature_store_name.replace('_featurestore','')}.{fg.name}_{fg.version}"  # featurestore.name_version
        featuregroups[fg._id] = fg_name
        filters = query._filter

        features = {}
        features[fg_name] = set([feat._name for feat in query._left_features])

        if fg.event_time:
            features[fg_name].update([fg.event_time])
        if fg.primary_key:
            features[fg_name].update(fg.primary_key)
        for join in query._joins:
            join_fg = join._query._left_feature_group
            join_fg_name = f"{join_fg.feature_store_name.replace('_featurestore','')}.{join_fg.name}_{join_fg.version}"  # featurestore.name_version
            left_on = join._on if len(join._on) > 0 else join._left_on
            right_on = join._on if len(join._on) > 0 else join._right_on

            self._update_features(features, fg_name, [feat._name for feat in left_on])
            self._update_features(
                features, join_fg_name, [feat._name for feat in right_on]
            )
            (
                join_featuregroups,
                join_features,
                join_filters,
            ) = self._collect_featuregroups_features_and_filters_rec(join._query)
            featuregroups.update(join_featuregroups)
            for join_fg_name in join_features:
                self._update_features(
                    features, join_fg_name, join_features[join_fg_name]
                )
            filters = (filters & join_filters) if join_filters is not None else filters

        return featuregroups, features, filters

    def _filter_to_expression(self, filters, featuregroups):
        if not filters:
            return None
        return self._resolve_logic(filters, featuregroups)

    def _resolve_logic(self, logic, featuregroups):
        filter_expression = {}
        filter_expression["type"] = "logic"
        filter_expression["logic_type"] = logic._type
        if logic._left_f:
            left_filter = self._resolve_filter(logic._left_f, featuregroups)
        elif logic._left_l:
            left_filter = self._resolve_logic(logic._left_l, featuregroups)
        else:
            left_filter = None
        filter_expression["left_filter"] = left_filter

        if logic._right_f:
            right_filter = self._resolve_filter(logic._right_f, featuregroups)
        elif logic._right_l:
            right_filter = self._resolve_logic(logic._right_l, featuregroups)
        else:
            right_filter = None
        filter_expression["right_filter"] = right_filter

        return filter_expression

    def _resolve_filter(self, filter, featuregroups):
        filter_expression = {}
        filter_expression["type"] = "filter"
        filter_expression["condition"] = filter._condition
        filter_expression["value"] = filter._value
        feature_name = filter._feature._name
        feature_type = filter._feature._type
        feature_group_name = featuregroups[filter._feature._feature_group_id]
        filter_expression["feature"] = f"{feature_group_name}.{feature_name}"
        filter_expression["numeric"] = (
            feature_type in ArrowFlightClient.FILTER_NUMERIC_TYPES
        )

        return filter_expression

    def _info_to_ticket(self, info):
        return info.endpoints[0].ticket
