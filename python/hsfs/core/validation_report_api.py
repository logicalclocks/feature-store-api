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

from hsfs import client
from hsfs.validation_report import ValidationReport


class ValidationReportApi:
    def __init__(self, feature_store_id):
        """Validation Report endpoints for the featuregroup resource.

        :param feature_store_id: id of the respective featurestore
        :type feature_store_id: int
        :param feature_group_id: id of the respective featuregroup
        :type feature_group_id: int
        """
        self._feature_store_id = feature_store_id

    def create(self, feature_group_id, validation_report):
        """Create an validation report attached to a featuregroup.

        :param validation_report: validation report object to be created for a featuregroup
        :type validation_report: `ValidationReport`
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            feature_group_id,
            "validationreport",
        ]

        headers = {"content-type": "application/json"}
        payload = validation_report.json()
        return ValidationReport.from_response_json(
            _client._send_request("PUT", path_params, headers=headers, data=payload)
        )

    def delete(self, feature_group_id, validation_report_id):
        """Delete the validation report attached to a featuregroup."""
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            feature_group_id,
            "validationreport",
            validation_report_id,
        ]

        _client._send_request("DELETE", path_params)

    def get_last(self, feature_group_id):
        """Gets the latest Validation Report of a featuregroup."""
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            feature_group_id,
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
        )[0]

    def get_all(self, feature_group_id):
        """Get the validation report attached to a featuregroup.

        :return: validation report
        :rtype: list[dict]
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            feature_group_id,
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
