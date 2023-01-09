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
from hsfs import expectation_suite as es
from hsfs.core.variable_api import VariableApi


class ExpectationSuiteApi:
    def __init__(self, feature_store_id: int, feature_group_id: int):
        """Expectation Suite endpoints for the Feature Group resource.

        :param feature_store_id: id of the respective Feature Store
        :type feature_store_id: int
        :param feature_group_id: id of the respective Feature Group
        :type feature_group_id: int
        """
        self._feature_store_id = feature_store_id
        self._feature_group_id = feature_group_id
        self._variable_api = VariableApi()

    def create(self, expectation_suite: es.ExpectationSuite) -> es.ExpectationSuite:
        """Create an expectation suite attached to a Feature Group.

        :param expectation_suite: expectation suite object to be created for a Feature Group
        :type expectation_suite: `ExpectationSuite`
        :return: the created expectation suite
        :rtype: ExpectationSuite
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

        major, minor = self._variable_api.parse_major_and_minor(
            self._variable_api.get_version("hopsworks")
        )
        method = "POST"
        if major == "3" and minor == "0":
            method = "PUT"

        headers = {"content-type": "application/json"}
        payload = expectation_suite.json()
        return es.ExpectationSuite.from_response_json(
            _client._send_request(method, path_params, headers=headers, data=payload)
        )

    def update(self, expectation_suite: es.ExpectationSuite) -> es.ExpectationSuite:
        """Update an expectation suite attached to a Feature Group.

        :param expectation_suite: expectation suite object to be created for a Feature Group
        :type expectation_suite: `ExpectationSuite`
        :return: the updated expectation suite
        :rtype: ExpectationSuite
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
        ]

        headers = {"content-type": "application/json"}
        payload = expectation_suite.json()

        major, minor = self._variable_api.parse_major_and_minor(
            self._variable_api.get_version("hopsworks")
        )
        method = "PUT"
        if major == "3" and minor == "0":
            method = "POST"
            del path_params[-1]

        return es.ExpectationSuite.from_response_json(
            _client._send_request(method, path_params, headers=headers, data=payload)
        )

    def update_metadata(
        self, expectation_suite: es.ExpectationSuite
    ) -> es.ExpectationSuite:
        """Update the metadata of an expectation suite attached to a Feature Group.

        :param expectation_suite: expectation suite object to be updated
        :type expectation_suite: `ExpectationSuite`
        :return: the expectation suite with updated metadata.
        :rtype: ExpectationSuite
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
            "metadata",
        ]

        headers = {"content-type": "application/json"}
        payload = expectation_suite.json()

        major, minor = self._variable_api.parse_major_and_minor(
            self._variable_api.get_version("hopsworks")
        )
        method = "PUT"
        if major == "3" and minor == "0":
            method = "POST"
            del path_params[-1]
            del path_params[-1]

        return es.ExpectationSuite.from_response_json(
            _client._send_request(method, path_params, headers=headers, data=payload)
        )

    def delete(self, expectation_suite_id: int) -> None:
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
            expectation_suite_id,
        ]

        major, minor = self._variable_api.parse_major_and_minor(
            self._variable_api.get_version("hopsworks")
        )
        if major == "3" and minor == "0":
            del path_params[-1]

        _client._send_request("DELETE", path_params)

    def get(self) -> Optional[es.ExpectationSuite]:
        """Get the expectation suite attached to a Feature Group.

        :return: fetched expectation suite attached to the FeatureG Group
        :rtype: ExpectationSuite || None
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

        return es.ExpectationSuite.from_response_json(
            _client._send_request("GET", path_params)
        )
