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
from hsfs.core.expectation_suite_api import ExpectationSuiteApi
from hsfs.core.expectation_engine import ExpectationEngine
from hsfs import client, util
from hsfs.ge_expectation import GeExpectation
from hsfs.expectation_suite import ExpectationSuite


class ExpectationSuiteEngine:
    def __init__(self, feature_store_id : int, feature_group_id: int, expectation_suite_id: Optional[int] = None):
        """Expectation Suite engine.

        :param feature_store_id: id of the respective featurestore
        :type feature_store_id: int
        :param feature_group_id: id of the respective featuregroup
        :type feature_group_id: int
        """
        self._feature_store_id = feature_store_id
        self._feature_group_id = feature_group_id
        self._expectation_suite_api = ExpectationSuiteApi(
            feature_store_id=feature_store_id,
            feature_group_id=feature_group_id
        )
        if expectation_suite_id:
            self._expectation_suite_id = expectation_suite_id
            # self._init_expectation_engine()
        else:
            self._expectation_suite_id = None
            self._expectation_engine = None

    def save(self, expectation_suite: ExpectationSuite) -> ExpectationSuite:
        saved_suite = self._expectation_suite_api.create(expectation_suite)
        url = self._get_expectation_suite_url()
        print(f"Attached expectation suite to featuregroup, edit it at {url}")
        self._expectation_suite_id = saved_suite.id
        # self._init_expectation_engine()
        return saved_suite

    def get(self) -> ExpectationSuite:
        fetched_suite = self._expectation_suite_api.get()
        self._expectation_suite_id = fetched_suite.id
        # self._init_expectation_engine()
        return fetched_suite

    def delete(self) -> None:
        self._expectation_suite_api.delete()
        self._expectation_engine = None

    # # Emulate GE single expectation api to edit list of expectations
    # def _init_expectation_engine(self, expectation_suite_id: Optional[int] = None) -> None:
    #     if (self._expectation_suite_id == None and expectation_suite_id == None):
    #         raise ValueError(
    #             "The expectation suite id must be provided to edit expectations"
    #         )
    #     elif self._expectation_suite_id == None:
    #         self._expectation_suite_id = expectation_suite_id

    #     if self._expectation_engine == None:
    #         self._expectation_engine = ExpectationEngine(
    #             feature_store_id=self._feature_store_id,
    #             feature_group_id=self._feature_group_id,
    #             expectation_suite_id=self._expectation_suite_id
    #         )

    # def add_expectation(self, expectation: GeExpectation) -> GeExpectation:
    #     self._init_expectation_engine()
    #     return self._expectation_engine.save(expectation=expectation)

    # def replace_expectation(self, expectation: GeExpectation) -> GeExpectation:
    #     self._init_expectation_engine()
    #     return self._expectation_engine.update(expectation=expectation)

    # def remove_expectation(self, expectation_id: int) -> None:
    #     self._init_expectation_engine()
    #     self._expectation_engine.delete(expectation_id=expectation_id)
    # # End of single expectation api

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
