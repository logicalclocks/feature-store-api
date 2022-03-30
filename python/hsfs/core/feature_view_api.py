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

from hsfs import client, feature_view


class FeatureViewApi:
    _POST = "POST"
    _GET = "GET"
    _DELETE = "DELETE"
    _VERSION = "version"

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


