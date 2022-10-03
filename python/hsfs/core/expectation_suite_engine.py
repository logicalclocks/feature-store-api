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

from hsfs.core import expectation_suite_api, expectation_engine
from hsfs import client, util
from hsfs.ge_expectation import GeExpectation


class ExpectationSuiteEngine:
    def __init__(self, feature_store_id):
        """Expectation Suite engine.

        :param feature_store_id: id of the respective featurestore
        :type feature_store_id: int
        """
        self._feature_store_id = feature_store_id
        self._expectation_suite_api = expectation_suite_api.ExpectationSuiteApi(
            feature_store_id
        )
        self._expectation_engine = None

    def save(self, feature_group, expectation_suite):
        saved_suite = self._expectation_suite_api.create(
            feature_group.id, expectation_suite
        )
        url = self._get_expectation_suite_url(feature_group=feature_group)
        print(f"Attached expectation suite to featuregroup, edit it at {url}")
        return saved_suite

    def get(self, feature_group):
        return self._expectation_suite_api.get(feature_group.id)

    def delete(self, feature_group):
        self._expectation_suite_api.delete(feature_group.id)

    # Emulate GE single expectation api to edit list of expectations
    def _init_expectation_engine(self, feature_group):
        if self._expectation_engine == None:
            self._expectation_engine = expectation_engine.ExpectationEngine(
                feature_store_id=self._feature_store_id,
                feature_group_id=feature_group.id,
                expectation_suite_id=self.get(feature_group).id
            )

    def add_expectation(self, feature_group, expectation: GeExpectation):
        self._init_expectation_engine(feature_group=feature_group)
        
        return self._expectation_engine.save(expectation=expectation)

    def replace_expectation(self, feature_group, expectation: GeExpectation):
        self._init_expectation_engine(feature_group=feature_group)
        return self._expectation_engine.update(expectation=expectation)

    def remove_expectation(self, feature_group, expectation_id: int):
        self._init_expectation_engine(feature_group=feature_group)
        self._expectation_engine.delete(expectation_id=expectation_id)
    # End of single expectation api

    def _get_expectation_suite_url(self, feature_group):
        """Build url to land on Hopsworks UI page which summarizes validation results"""
        sub_path = (
            "/p/"
            + str(client.get_instance()._project_id)
            + "/fs/"
            + str(feature_group.feature_store_id)
            + "/fg/"
            + str(feature_group.id)
        )
        return util.get_hostname_replaced_url(sub_path)
