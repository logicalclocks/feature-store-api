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

from hsfs import client
from hsfs.expectation_suite import ExpectationSuite


class ExpectationSuiteApi:
    def __init__(self, feature_store_id):
        """Expectation Suite endpoints for the featuregroup resource.
        :param feature_store_id: id of the respective featurestore
        :type feature_store_id: int
        :param feature_group_id: id of the respective featuregroup
        :type feature_group_id: int
        """
        self._feature_store_id = feature_store_id

    def create(self, feature_group_id, expectation_suite):
        """Create an expectation suite attached to a featuregroup.
        :param expectation_suite: expectation suite object to be created for a featuregroup
        :type expectation_suite: `ExpectationSuite`
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            feature_group_id,
            "expectationsuite",
        ]

        headers = {"content-type": "application/json"}
        payload = expectation_suite.json()
        return ExpectationSuite.from_response_json(
            _client._send_request("PUT", path_params, headers=headers, data=payload)
        )

    def delete(self, feature_group_id):
        """Delete the expectation suite attached to a featuregroup."""
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            feature_group_id,
            "expectationsuite",
        ]

        _client._send_request("DELETE", path_params)

    def get(self, feature_group_id):
        """Get the expectation suite attached to a feature group.

        :return: expectation suite
        :rtype: dict
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            feature_group_id,
            "expectationsuite",
        ]

        return ExpectationSuite.from_response_json(
            _client._send_request("GET", path_params)
        )
