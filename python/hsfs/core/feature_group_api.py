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
from hsfs import feature_group, tag


class FeatureGroupApi:
    def __init__(self, feature_store_id):
        self._feature_store_id = feature_store_id

    def save(self, feature_group_instance):
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
        """Get feature store with specific id or name.
        :param identifier: id or name of the feature store
        :type identifier: int, str
        :return: the featurestore metadata
        :rtype: FeatureStore
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
        """Delete content of the feature group. It simulates the overwrite insert mode

        Args:
            feature_group feature_group: the feature for which to delete the content 
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
        """Drop a feature group from the feature store

        Args:
            feature_group_instance feature_group: the feature_group to drop 
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

    def add_tag(self, feature_group_instance, name, value=None):
        """Attach a tag to a feature group 

        Args:
            feature_group_instance feature_group: the feature group to which to attach the tag 
            name (string): the name of the tag to attach 
            value (string): the value of the tag. can be none 
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            feature_group_instance.id,
            "tags",
            name,
        ]
        query_params = {"value": value} if value else None
        _client._send_request("PUT", path_params, query_params=query_params)

    def delete_tag(self, feature_group_instance, name):
        """Remove a tag from a feature group

        Args:
            feature_group_instance feature_group: the feature group from which to delete the tag 
            name (string): the name of the tag to remove 
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            feature_group_instance.id,
            "tags",
            name,
        ]
        _client._send_request("DELETE", path_params)

    def get_tags(self, feature_group_instance, name=None):
        """[summary]

        Args:
            feature_group_instance feature_group: the feature group for which to retrieve the tags 
            name ([type], optional): [description]. Defaults to None.
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            feature_group_instance.id,
            "tags",
        ]

        if name:
            path_params.extend(["name", name])

        return tag.Tag.from_response_json(_client._send_request("GET", path_params))
