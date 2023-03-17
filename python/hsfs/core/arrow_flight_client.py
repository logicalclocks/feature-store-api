#
#   Copyright 2023 Logical Clocks AB
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

import pickle
import pyarrow
import pyarrow.flight
from pyarrow.flight import FlightServerError
from hsfs import client
from hsfs import feature_group
from hsfs.core.variable_api import VariableApi


class ArrowFlightClient:

    instance = None

    @classmethod
    def get_instance(cls):
        if not cls.instance:
            cls.instance = ArrowFlightClient()
        return cls.instance

    def __init__(self):
        self._client = client.get_instance()
        self._variable_api = VariableApi()
        self._is_enabled = True #self._variable_api.get_flyingduck_enabled() # TODO: enable this when new backend is deployed
        if self._is_enabled:
            self._initialize_connection()

    def _initialize_connection(self):
        host_ip = self._client._get_host_port_pair()[0]
        host_url = f"grpc+tls://{host_ip}:5005"
        (tls_root_certs, cert_chain, private_key) = self._extract_certs(self._client)
        self._connection = pyarrow.flight.FlightClient(
            location=host_url,
            tls_root_certs=tls_root_certs,
            cert_chain=cert_chain,
            private_key=private_key,
            override_hostname="flyingduck.service.consul"
        )
        self._register_certificates()

    def is_enabled(self):
        return self._is_enabled

    def _handle_afs_errors(method):
        def afs_error_handler_wrapper(*args, **kw):
            try:
                return method(*args, **kw)
            except FlightServerError as e:
                message = str(e)
                if "Please register client certificates first." in message:
                    self = args[0]
                    self._register_certificates()
                    return method(*args, **kw)
                else:
                    raise
        return afs_error_handler_wrapper

    def _extract_certs(self, client):
        with open(client._get_ca_chain_path(),"rb") as f:
            tls_root_certs = f.read()
        with open(client._get_client_cert_path(),"r") as f:
            cert_chain = f.read()
        with open(client._get_client_key_path(),"r") as f:
            private_key = f.read()
        return tls_root_certs, cert_chain, private_key

    def _register_certificates(self):
        with open(self._client._get_jks_key_store_path(), "rb") as f:
            kstore = f.read()
        with open(self._client._get_jks_trust_store_path(), "rb") as f:
            tstore = f.read()
        cert_key = self._client._cert_key
        certificates_pickled = pickle.dumps((kstore,tstore,cert_key)) # TODO dump kstore, tstore, priv-key
        certificates_pickled_buf = pyarrow.py_buffer(certificates_pickled)
        action = pyarrow.flight.Action("register-client-certificates", certificates_pickled_buf)
        try:
            self._connection.do_action(action)
        except pyarrow.lib.ArrowIOError as e:
            print("Error calling action:", e)

    def _get_dataset(self, descriptor):
        info = self._connection.get_flight_info(descriptor)
        reader = self._connection.do_get(self._info_to_ticket(info))
        return reader.read_pandas()

    @_handle_afs_errors
    def read_query(self, query, query_str):
        if not self._is_enabled:
            raise Exception("Arrow Flight Service is not enabled.")
        query_encoded = pickle.dumps(self._get_query_object(query, query_str))
        descriptor = pyarrow.flight.FlightDescriptor.for_command(query_encoded)
        return self._get_dataset(descriptor)

    @_handle_afs_errors
    def get_training_dataset(self, feature_view, tds_version=1):
        if not self._is_enabled:
            raise Exception("Arrow Flight Service is not enabled.")
        path = self._path_from_feature_view(feature_view, tds_version)
        return self.read_path(self, path)

    @_handle_afs_errors
    def read_path(self, path):
        if not self._is_enabled:
            raise Exception("Arrow Flight Service is not enabled.")
        descriptor = pyarrow.flight.FlightDescriptor.for_path(path)
        return self._get_dataset(descriptor)

    @_handle_afs_errors
    def create_training_dataset(self, feature_view, tds_version=1):
        if not self._is_enabled:
            raise Exception("Arrow Flight Service is not enabled.")
        training_dataset_metadata = self._training_dataset_metadata_from_feature_view(feature_view, tds_version)
        try:
            training_dataset_encoded = pickle.dumps(training_dataset_metadata)
            buf = pyarrow.py_buffer(training_dataset_encoded)
            action = pyarrow.flight.Action("create-training-dataset", buf)
            for result in self._connection.do_action(action):
                return result.body.to_pybytes()
        except pyarrow.lib.ArrowIOError as e:
            print("Error calling action:", e)

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
        training_dataset_metadata["query"] = self._get_query_object(feature_view.query, feature_view.query.to_string())
        training_dataset_metadata["featurestore_name"] = feature_view.query._left_feature_group.feature_store_name.replace("_featurestore","")

        return training_dataset_metadata

    def is_supported(self, query):
        query_supported = (isinstance(query._left_feature_group, feature_group.FeatureGroup) and
                                      query._left_feature_group.time_travel_format == "HUDI" and
                                      (query._left_feature_group_start_time is None
                                       or query._left_feature_group_start_time == 0) and
                                      query._left_feature_group_end_time is None)
        for j in query._joins:
            query_supported &= self.is_supported(j._query)

        return query_supported

    def _get_query_object(self, query, query_str):
        query_string = query_str.replace(f"`{query._left_feature_group.feature_store_name}`.`",
                                         f"`{query._left_feature_group.feature_store_name.replace('_featurestore','')}.").replace("`","\"")

        featuregroups, features, filters = self._collect_featuregroups_features_and_filters(query)

        query = {"query_string": query_string, "featuregroups": featuregroups, "features": features, "filters": filters}
        return query

    def _collect_featuregroups_features_and_filters(self, query):
        featuregroups = {}
        fg = query._left_feature_group
        fg_name = f"{fg.feature_store_name.replace('_featurestore','')}.{fg.name}_{fg.version}" # featurestore.name_version
        featuregroups[fg._id] = fg_name
        filters = self._filter_to_expression(query._filter, featuregroups)

        features = {}
        features[fg_name] = [feat._name for feat in query._left_features]

        for join in query._joins:
            join_featuregroups, join_features, join_filters = self._collect_featuregroups_features_and_filters(join._query)
            featuregroups.update(join_featuregroups)
            features.update(join_features)
            filters = (filters & join_filters) if join_filters is not None else filters

        return featuregroups, features, filters

    def _filter_to_expression(self, filters, featuregroups):
        if not filters:
            return None
        filter_expression = self._resolve_logic(filters, featuregroups)
        return filter_expression

    def _resolve_logic(self, l, featuregroups):
        logic_type = l._type
        if l._left_f:
            left_filter = self._resolve_filter(l._left_f, featuregroups)
        elif l._left_l:
            left_filter = self._resolve_logic(l._left_l, featuregroups)
        else:
            left_filter = None

        if l._right_f:
            right_filter = self._resolve_filter(l._right_f, featuregroups)
        elif l._right_l:
            right_filter = self._resolve_logic(l._right_l, featuregroups)
        else:
            right_filter = None

        filter_expression = (left_filter, logic_type, right_filter)

        return filter_expression

    def _resolve_filter(self, filter, featuregroups):
        condition = filter._condition
        value = filter._value
        feature_name = filter._feature._name
        feature_type = filter._feature._type
        feature_group_name = featuregroups[filter._feature._feature_group_id]
        feature = f"{feature_group_name}.{feature_name}"
        numeric_types = ["bigint", "tinyint", "smallint", "int", "float", "double"]
        numeric = feature_type in numeric_types

        filter_expression = (feature, condition, value, numeric)
        return filter_expression

    def _info_to_ticket(self, info):
        return info.endpoints[0].ticket