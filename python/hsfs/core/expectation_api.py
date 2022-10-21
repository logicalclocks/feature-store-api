#
#   Copyright 2022 Hopsworks AB
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
from hsfs.ge_expectation import GeExpectation
from typing import List


class ExpectationApi:
    def __init__(
        self, feature_store_id: int, feature_group_id: int, expectation_suite_id: int
    ):
        """Expectation Suite endpoints for the featuregroup resource.
        :param feature_store_id: id of the respective Feature Store
        :type feature_store_id: int
        :param feature_group_id: id of the respective Feature Group
        :type feature_group_id: int
        :param expectation_suite_id: id of the respective Expectation Suite
        :type expectation_suite_id: int
        """
        self._feature_store_id = feature_store_id
        self._feature_group_id = feature_group_id
        self._expectation_suite_id = expectation_suite_id

    def create(self, expectation: GeExpectation) -> GeExpectation:
        """Create an expectation suite attached to a Feature Group.
        :param expectation: Expectation object to be appended to an Expectation Suite
        :type expectation: `GeExpectation`

        :return: expectation
        :rtype: `GeExpectation`
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
            self._expectation_suite_id,
            "expectations",
        ]

        headers = {"content-type": "application/json"}
        payload = expectation.json()
        return GeExpectation.from_response_json(
            _client._send_request("POST", path_params, headers=headers, data=payload)
        )

    def update(self, expectation: GeExpectation) -> GeExpectation:
        """Update an Expectation of an Expectation Suite attached to a Feature Group.
        :param expectation: Expectation object to be appended to an Expectation Suite
        :type expectation: `GeExpectation`

        :return: expectation
        :rtype: `GeExpectation`
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
            self._expectation_suite_id,
            "expectations",
            expectation.id,
        ]

        headers = {"content-type": "application/json"}
        payload = expectation.json()
        return GeExpectation.from_response_json(
            _client._send_request("PUT", path_params, headers=headers, data=payload)
        )

    def delete(self, expectation_id: int) -> None:
        """Delete the Expectation with expectation_id from the Expectation Suite attached to a Feature Group.
        :param expectation_id: id of the Expectation to delete
        :type expectation_id: `int`
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
            self._expectation_suite_id,
            "expectations",
            expectation_id,
        ]

        _client._send_request("DELETE", path_params)

    def get(self, expectation_id: int) -> GeExpectation:
        """Get an expectation attached to a feature group.

        :return: expectation
        :rtype: `GeExpectation`
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
            self._expectation_suite_id,
            "expectations",
            expectation_id,
        ]

        return GeExpectation.from_response_json(
            _client._send_request("GET", path_params)
        )

    def get_expectations_by_suite_id(self) -> List[GeExpectation]:
        """Get an expectation attached to a feature group.

        :return: expectation
        :rtype: `GeExpectation`
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
            self._expectation_suite_id,
            "expectations",
        ]

        return GeExpectation.from_response_json(
            _client._send_request("GET", path_params)
        )
