#
#   Copyright 2020 Logical Clocks AB
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

from hsfs import client
from hsfs import feature_group


class FeatureGroupApi:
    def __init__(self, feature_store_id):
        self._feature_store_id = feature_store_id

    def save(self, feature_group_instance):
        """Save feature group metadata to the feature store.

        :param feature_group_instance: metadata object of feature group to be
            saved
        :type feature_group_instance: FeatureGroup
        :return: updated metadata object of the feature group
        :rtype: FeatureGroup
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
        ]
        headers = {"content-type": "application/json"}
        return feature_group_instance.update_from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                data=feature_group_instance.json(),
            ),
        )

    def get(self, name, version):
        """Get the metadata of a feature group with a certain name and version.

        :param name: name of the feature group
        :type name: str
        :param version: version of the feature group
        :type version: int
        :return: feature group metadata object
        :rtype: FeatureGroup
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            name,
        ]
        query_params = {"version": version}
        return feature_group.FeatureGroup.from_response_json(
            _client._send_request("GET", path_params, query_params)[0],
        )

    def delete_content(self, feature_group_instance):
        """Delete the content of a feature group.

        This endpoint serves to simulate the overwrite/insert mode.

        :param feature_group_instance: metadata object of feature group to clear
            the content for
        :type feature_group_instance: FeatureGroup
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            feature_group_instance.id,
            "clear",
        ]
        _client._send_request("POST", path_params)

    def delete(self, feature_group_instance):
        """Drop a feature group from the feature store.

        Drops the metadata and data of a version of a feature group.

        :param feature_group_instance: metadata object of feature group
        :type feature_group_instance: FeatureGroup
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            feature_group_instance.id,
        ]
        _client._send_request("DELETE", path_params)
