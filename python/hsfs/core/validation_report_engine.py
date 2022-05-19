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

from hsfs.core import validation_report_api


class ValidationReportEngine:
    def __init__(self, feature_store_id):
        """Validation Report engine.

        :param feature_store_id: id of the respective featurestore
        :param feature_group_id: id of the featuregroup it is attached to
        :type feature_store_id: int
        :type feature_group_id: int
        """
        self._feature_store_id = feature_store_id
        self._validation_report_api = validation_report_api.ValidationReportApi(
            feature_store_id
        )

    def save(self, feature_group, validation_report):
        return self._validation_report_api.create(feature_group.id, validation_report)

    def get_last(self, feature_group):
        """Get the most recent Validation Report of a Feature Group."""
        return self._validation_report_api.get_last(feature_group.id)

    def get_all(self, feature_group):
        """Get all Validation Report of a FeaturevGroup."""
        return self._validation_report_api.get_all(feature_group.id)

    def delete(self, feature_group, validation_report):
        self._validation_report_api.delete(feature_group.id, validation_report.id)
