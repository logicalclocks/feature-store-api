#
#   Copyright 2023 Hopsworks AB
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

from typing import List
from hsfs import client
import feature_monitoring_result as fmc


class FeatureMonitoringResultApi:
    def __init__(self, feature_store_id: int, feature_group_id: int):
        """Feature Monitoring Result endpoints for the Feature Group resource.

        :param feature_store_id: id of the respective Feature Store
        :type feature_store_id: int
        :param feature_group_id: id of the respective Feature Group
        :type feature_group_id: int
        """
        self._feature_store_id = feature_store_id
        self._feature_group_id = feature_group_id

    def create(
        self, fm_result: fmc.FeatureMonitoringResult
    ) -> fmc.FeatureMonitoringResult:
        """Create an feature monitoring result attached to the Feature of a Feature Group.

        :param fm_result: feature monitoring result object to be attached to a Feature
        :type fm_result: `FeatureMonitoringResult`
        :return: the created feature monitoring result
        :rtype: FeatureMonitoringResult
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            self._feature_group_id,
            "featuremonitoring",
            "result",
        ]

        headers = {"content-type": "application/json"}
        payload = fm_result.json()
        return fmc.FeatureMonitoringResult.from_response_json(
            _client._send_request("POST", path_params, headers=headers, data=payload)
        )

    def delete(self, result_id: int) -> None:
        """Delete the Feature Monitoring result attached to a Feature."""
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            self._feature_group_id,
            "featuremonitoring",
            "result",
            result_id,
        ]

        _client._send_request("DELETE", path_params)

    def get_all(self, config_id) -> List[fmc.FeatureMonitoringResult]:
        """Get the Feature Monitoring Result attached to a Feature.

        :param config_id: Id of the feature monitoring config for which to fetch all results
        :type config_id: int
        :return: fetched feature monitoring result attached to the Feature Group
        :rtype: FeatureMonitoringResult || None
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            self._feature_group_id,
            "featuremonitoring",
            "result",
            config_id,
        ]

        return fmc.FeatureMonitoringResult.from_response_json(
            _client._send_request("GET", path_params)
        )
