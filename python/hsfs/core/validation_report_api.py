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

from typing import Union, List
from hsfs import client
from hsfs.validation_report import ValidationReport
from hsfs.core.variable_api import VariableApi


class ValidationReportApi:
    def __init__(self, feature_store_id: int, feature_group_id: int):
        """Validation Report endpoints for the featuregroup resource.

        :param feature_store_id: id of the respective featurestore
        :type feature_store_id: int
        :param feature_group_id: id of the respective featuregroup
        :type feature_group_id: int
        """
        self._feature_store_id = feature_store_id
        self._feature_group_id = feature_group_id
        self._variable_api = VariableApi()

    def create(self, validation_report: ValidationReport) -> ValidationReport:
        """Create an validation report attached to a featuregroup.

        :param validation_report: validation report object to be created for a featuregroup
        :type validation_report: `ValidationReport`

        :return: persisted validation report
        :rtype: `ValidationReport`
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            self._feature_group_id,
            "validationreport",
        ]

        major, minor = self._variable_api.parse_major_and_minor(
            self._variable_api.get_version("hopsworks")
        )
        if major == "3" and minor == "0":
            # You must bypass the setter otherwise it ends up as "UNKNOWN"
            validation_report._ingestion_result = None

        headers = {"content-type": "application/json"}
        payload = validation_report.json()

        return ValidationReport.from_response_json(
            _client._send_request("PUT", path_params, headers=headers, data=payload)
        )

    def delete(self, validation_report_id: int) -> None:
        """Delete the validation report attached to a featuregroup."""
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            self._feature_group_id,
            "validationreport",
            validation_report_id,
        ]

        _client._send_request("DELETE", path_params)

    def get_last(self) -> ValidationReport:
        """Gets the latest Validation Report of a featuregroup.

        :return: latest validation report
        :rtype: `ValidationReport`
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            self._feature_group_id,
            "validationreport",
        ]
        headers = {"content-type": "application/json"}
        query_params = {
            "sort_by": "validation_time:desc",
            "offset": 0,
            "limit": 1,
            "fields": "content",
        }

        return ValidationReport.from_response_json(
            _client._send_request("GET", path_params, query_params, headers=headers)
        )

    def get_all(self) -> Union[List[ValidationReport], ValidationReport]:
        """Get the validation report attached to a featuregroup.

        :return: validation report
        :rtype: Union[List[ValidationReport], ValidationReport]
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            self._feature_group_id,
            "validationreport",
        ]
        headers = {"content-type": "application/json"}
        query_params = {
            "sort_by": "validation_time:desc",
            "offset": 0,
            "fields": "content",
        }

        return ValidationReport.from_response_json(
            _client._send_request("GET", path_params, query_params, headers=headers)
        )
