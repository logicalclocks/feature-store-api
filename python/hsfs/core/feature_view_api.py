#
#   Copyright 2022 Logical Clocks AB
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

from hsfs import client, feature_view, transformation_function_attached
from hsfs.constructor import serving_prepared_statement


class FeatureViewApi:
    _POST = "POST"
    _GET = "GET"
    _DELETE = "DELETE"
    _VERSION = "version"
    _QUERY = "query"
    _BATCH = "batch"
    _PREPARED_STATEMENT = "preparedstatements"
    _TRANSFORMATION = "transformation"
    _FUNCTIONS = "functions"

    def __init__(self, feature_store_id):
        self._feature_store_id = feature_store_id
        self._client = client.get_instance()
        self._base_path = [
            "project",
            self._client._project_id,
            "featurestores",
            self._feature_store_id,
            "featureview",
        ]

    def post(self, feature_view_obj):
        headers = {"content-type": "application/json"}
        return feature_view_obj.update_from_response_json(
            self._client._send_request(
                self._POST,
                self._base_path,
                headers=headers,
                data=feature_view_obj.json(),
            )
        )

    def get_by_name(self, name):
        path = self._base_path + [name]
        return [feature_view.FeatureView.from_response_json(fv)
                for fv in self._client._send_request(
                self._GET, path, {"expand": "query"})["items"]
                ]

    def get_by_name_version(self, name, version):
        path = self._base_path + [name, self._VERSION, version]
        return feature_view.FeatureView.from_response_json(
            self._client._send_request(self._GET, path, {"expand": "query"})
        )

    def delete_by_name(self, name):
        path = self._base_path + [name]
        self._client._send_request(self._DELETE, path)

    def delete_by_name_version(self, name, version):
        path = self._base_path + [name, self._VERSION, version]
        self._client._send_request(self._DELETE, path)

    def get_batch_query(self, name, version, start_time, end_time,
                        with_label=False, is_python_engine=False):
        path = self._base_path + \
               [name, self._VERSION, version, self._QUERY, self._BATCH]
        return self._client._send_request(self._GET, path,
                                          {"start_time": start_time,
                                           "end_time": end_time,
                                           "with_label": with_label,
                                           "is_hive_engine": is_python_engine}
                                          )

    def get_serving_prepared_statement(self, name, version, batch):
        path = self._base_path + \
               [name, self._VERSION, version, self._PREPARED_STATEMENT]
        headers = {"content-type": "application/json"}
        query_params = {"batch": batch}
        return serving_prepared_statement.ServingPreparedStatement\
            .from_response_json(
            self._client._send_request(
                "GET", path, query_params, headers=headers)
        )

    def get_attached_transformation_fn(self, name, version):
        path = self._base_path + \
               [name, self._VERSION, version,
                self._TRANSFORMATION, self._FUNCTIONS]
        return transformation_function_attached.TransformationFunctionAttached.\
            from_response_json(self._client._send_request("GET", path))
