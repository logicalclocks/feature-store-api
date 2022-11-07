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

from typing import Optional
from hsfs.core import expectation_api

from hsfs.ge_expectation import GeExpectation


class ExpectationEngine:
    def __init__(
        self, feature_store_id: int, feature_group_id: int, expectation_suite_id: int
    ):
        """Expectation engine.

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
        self._expectation_api = expectation_api.ExpectationApi(
            feature_store_id, feature_group_id, expectation_suite_id
        )

    # CRUD operations
    def create(self, expectation: GeExpectation) -> GeExpectation:
        return self._expectation_api.create(expectation)

    def update(self, expectation: GeExpectation) -> GeExpectation:
        return self._expectation_api.update(expectation)

    def get(self, expectation_id: int) -> Optional[GeExpectation]:
        return self._expectation_api.get(expectation_id)

    def delete(self, expectation_id: int) -> None:
        self._expectation_api.delete(expectation_id)

    # End of CRUD operations

    def get_expectations_by_suite_id(self):
        return self._expectation_api.get_expectations_by_suite_id()

    def check_for_id(self, expectation: GeExpectation) -> None:
        if expectation.id:
            return
        else:
            if "expectationId" in expectation.meta.keys():
                expectation.id = int(expectation.meta["expectationId"])
            else:
                raise ValueError(
                    "The provided expectation has no id. Either set the id field or populate the meta field with an expectationId keyword."
                )
