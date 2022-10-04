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

from typing import Union, List
from hsfs.core.validation_report_api import ValidationReportApi
from hsfs import client, util

from hsfs.validation_report import ValidationReport


class ValidationReportEngine:
    def __init__(self, feature_store_id: int, feature_group_id: int):
        """Validation Report engine.

        :param feature_store_id: id of the respective featurestore
        :type feature_store_id: int
        :param feature_group_id: id of the featuregroup it is attached to
        :type feature_group_id: int
        """
        self._feature_store_id = feature_store_id
        self._feature_group_id = feature_group_id
        self._validation_report_api = ValidationReportApi(
            feature_store_id=feature_store_id,
            feature_group_id=feature_group_id
        )

    def save(self, validation_report: ValidationReport) -> ValidationReport:
        saved_report = self._validation_report_api.create(validation_report)
        url = self._get_validation_report_url()
        print(f"Validation Report saved successfully, explore a summary at {url}")
        return saved_report

    def get_last(self) -> ValidationReport:
        """Get the most recent Validation Report of a Feature Group."""
        url = self._get_validation_report_url()
        print(
            f"""Long reports can be truncated when fetching from Hopsworks.
        \nYou can download the full report at {url}"""
        )
        return self._validation_report_api.get_last()

    def get_all(self) -> Union[List[ValidationReport], ValidationReport]:
        """Get all Validation Report of a FeaturevGroup."""
        url = self._get_validation_report_url()
        print(
            f"""Long reports can be truncated when fetching from Hopsworks.
        \nYou can download full reports at {url}"""
        )
        return self._validation_report_api.get_all()

    def delete(self, validation_report_id: int):
        self._validation_report_api.delete(validation_report_id)

    def _get_validation_report_url(self):
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
