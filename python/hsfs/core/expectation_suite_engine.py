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
    def __init__(self, feature_store_id, feature_group_id):
        """Expectations engine.

        :param feature_store_id: id of the respective featurestore
        :param feature_group_id: id of the featuregroup it is attached to
        :type feature_store_id: int
        :type feature_group_id: int
        """
        self._feature_store_id = feature_store_id
        self._expectations_api = expectation_suite_api.ExpectationSuiteApi(feature_store_id)

    def save(self, expectation_suite):
        self._expectation_suite_api.create(expectation_suite)
