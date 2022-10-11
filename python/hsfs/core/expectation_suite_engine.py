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

from typing import Optional
from hsfs.core import expectation_suite_api
from hsfs import client, util
from hsfs import expectation_suite as es


class ExpectationSuiteEngine:
    def __init__(self, feature_store_id: int, feature_group_id: int):
        """Expectation Suite engine.

        :param feature_store_id: id of the respective featurestore
        :type feature_store_id: int
        :param feature_group_id: id of the respective featuregroup
        :type feature_group_id: int
        """
        self._feature_store_id = feature_store_id
        self._feature_group_id = feature_group_id
        self._expectation_suite_api = expectation_suite_api.ExpectationSuiteApi(
            feature_store_id=feature_store_id, feature_group_id=feature_group_id
        )

    def save(self, expectation_suite: es.ExpectationSuite) -> es.ExpectationSuite:
        if expectation_suite.id:
            return self.update(expectation_suite)
        else:
            return self.create(expectation_suite)

    def create(self, expectation_suite: es.ExpectationSuite) -> es.ExpectationSuite:
        saved_suite = self._expectation_suite_api.create(expectation_suite)

        url = self._get_expectation_suite_url()
        print(f"Attached expectation suite to Feature Group, edit it at {url}")

        return saved_suite

    def update(self, expectation_suite: es.ExpectationSuite) -> es.ExpectationSuite:
        saved_suite = self._expectation_suite_api.update(expectation_suite)

        url = self._get_expectation_suite_url()
        print(f"Updated expectation suite attached to Feature Group, edit it at {url}")

        return saved_suite

    def update_metadata(
        self, expectation_suite: es.ExpectationSuite
    ) -> es.ExpectationSuite:
        return self._expectation_suite_api.update_metadata(expectation_suite)

    def get(self) -> Optional[es.ExpectationSuite]:
        return self._expectation_suite_api.get()

    def delete(self) -> None:
        self._expectation_suite_api.delete()
        self._expectation_engine = None

    def _get_expectation_suite_url(self) -> str:
        """Build url to land on Hopsworks UI page which summarizes validation results"""
        sub_path = (
            "/p/"
            + str(client.get_instance()._project_id)
            + "/fs/"
            + str(self._feature_store_id)
            + "/fg/"
            + str(self._feature_group_id)
        )
        return util.get_hostname_replaced_url(sub_path)
