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

from typing import Optional
from hsfs import client
from hsfs.expectation_suite import ExpectationSuite


class ExpectationSuiteApi:
    def __init__(self, feature_store_id : int, feature_group_id : int):
        """Expectation Suite endpoints for the Feature Group resource.
        :param feature_store_id: id of the respective Feature Store
        :type feature_store_id: int
        :param feature_group_id: id of the respective Feature Group
        :type feature_group_id: int
        """
        self._feature_store_id = feature_store_id
        self._feature_group_id = feature_group_id

    def create(self, expectation_suite : ExpectationSuite) -> ExpectationSuite:
        """Create an expectation suite attached to a Feature Group.

        :param expectation_suite: expectation suite object to be created for a Feature Group
        :type expectation_suite: `ExpectationSuite`
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            self._feature_group_id,
            "expectationsuite",
        ]

        headers = {"content-type": "application/json"}
        payload = expectation_suite.json()
        return ExpectationSuite.from_response_json(
            _client._send_request("POST", path_params, headers=headers, data=payload)
        )

    def update(self, expectation_suite : ExpectationSuite) -> ExpectationSuite:
        """Update an expectation suite attached to a Feature Group.

        :param expectation_suite: expectation suite object to be created for a Feature Group
        :type expectation_suite: `ExpectationSuite`
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            self._feature_group_id,
            "expectationsuite",
            expectation_suite.id
        ]

        headers = {"content-type": "application/json"}
        payload = expectation_suite.json()
        return ExpectationSuite.from_response_json(
            _client._send_request("PUT", path_params, headers=headers, data=payload)
        )

    def update(self, expectation_suite : ExpectationSuite) -> ExpectationSuite:
        """Update the metadata of an expectation suite attached to a Feature Group.

        :param expectation_suite: expectation suite object to be updated
        :type expectation_suite: `ExpectationSuite`
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            self._feature_group_id,
            "expectationsuite",
            expectation_suite.id,
            "metadata"
        ]

        headers = {"content-type": "application/json"}
        payload = expectation_suite.json()
        return ExpectationSuite.from_response_json(
            _client._send_request("PUT", path_params, headers=headers, data=payload)
        )

    def delete(self) -> None:
        """Delete the expectation suite attached to a Feature Group."""
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            self._feature_group_id,
            "expectationsuite",
        ]

        _client._send_request("DELETE", path_params)

    def get(self) -> Optional[ExpectationSuite]:
        """Get the expectation suite attached to a Feature Group.

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
            self._feature_group_id,
            "expectationsuite",
        ]

        return ExpectationSuite.from_response_json(
            _client._send_request("GET", path_params)
        )
