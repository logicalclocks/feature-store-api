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

from hsfs.core import expectation_suite_api


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

    def save(self, feature_group, expectation_suite):
        return self._expectation_suite_api.create(feature_group.id, expectation_suite)

    def get(self, feature_group):
        return self._expectation_suite_api.get(feature_group.id)

    def delete(self, feature_group):
        self._expectation_suite_api.delete(feature_group.id)
