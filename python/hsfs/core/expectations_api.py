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

from hsfs import client, expectation


class ExpectationsApi:
    def __init__(self, feature_store_id, entity_type=None):
        """Expectations endpoint for `featurestores` and `featuregroups` resource.

        :param feature_store_id: id of the respective featurestore
        :type feature_store_id: int
        :param entity_type: "featuregroups"
        :type entity_type: str
        """
        self._feature_store_id = feature_store_id
        self._entity_type = entity_type

    def create(self, expectation):
        """Create and Feature Store expectation or Attach it by name to a Feature Group.

        :param expectation: expectation object to be created for a feature store
        :type expectation: `Expectation`
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "expectations",
        ]

        headers = {"content-type": "application/json"}
        payload = expectation.json() if expectation else None
        _client._send_request("PUT", path_params, headers=headers, data=payload)

    def attach(self, feature_group, name):
        """Attach a Feature Store expectation to a Feature Group.

        :param feature_group: metadata object of the instance to attach the expectation to
        :type feature_group: FeatureGroup
        :param name: name of the expectation to be attached
        :type name: str
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            self._entity_type,
            feature_group.id,
            "expectations",
            name,
        ]

        _client._send_request("PUT", path_params)

    def delete(self, name):
        """Delete a Feature Store expectation.

        :param name: name of the expectation to be deleted
        :type name: str
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "expectations",
            name,
        ]

        _client._send_request("DELETE", path_params)

    def detach(self, feature_group, name):
        """Detach a Feature Store expectation from a Feature Group.

        :param feature_group: metadata object of the instance to attach the expectation to
        :type feature_group: FeatureGroup
        :param name: name of the expectation to be attached
        :type name: str
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            self._entity_type,
            feature_group.id,
            "expectations",
            name,
        ]

        _client._send_request("DELETE", path_params)

    def get(self, name=None, feature_group=None):
        """Get the expectations of a feature store or feature group.

        Gets all feature store expectations if no feature group is specified.
        Gets all feature store or feature group expectations if no name is specified.

        :param name: expectation name
        :type name: str
        :param feature_group: feature group to get the expectations of
        :type feature_group: FeatureGroup
        :return: list of expectations
        :rtype: list of dict
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
        ]

        if feature_group is not None:
            path_params.extend([self._entity_type, feature_group.id, "expectations"])
        else:
            path_params.append("expectations")

        if name:
            path_params.append(name)

        return expectation.Expectation.from_response_json(
            _client._send_request("GET", path_params)
        )
