#
#   Copyright 2024 Hopsworks AB
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

from typing import Dict, List, Optional, Union
from hsfs import client
from hsfs.core.feature_monitoring_result import FeatureMonitoringResult


class FeatureMonitoringResultApi:
    """Feature Monitoring Result endpoints for the Feature Group resource.

    :param feature_store_id: id of the respective Feature Store
    :type feature_store_id: int
    :param feature_group_id: id of the feature group, if attaching a config to a feature group
    :type feature_group_id: int, optional
    :param feature_view_name: name of the feature view, if attaching a config to a feature view
    :type feature_view_name: str, optional
    :param feature_view_version: version of the feature view, if attaching a config to a feature view
    :type feature_view_version: int, optional
    """

    def __init__(
        self,
        feature_store_id: int,
        feature_group_id: Optional[int] = None,
        feature_view_name: Optional[str] = None,
        feature_view_version: Optional[int] = None,
        **kwargs,
    ):
        if feature_group_id is None:
            assert feature_view_name is not None
            assert feature_view_version is not None

        self._feature_store_id = feature_store_id
        self._feature_group_id = feature_group_id
        self._feature_view_name = feature_view_name
        self._feature_view_version = feature_view_version

    def create(
        self,
        fm_result: FeatureMonitoringResult,
    ) -> FeatureMonitoringResult:
        """Create an feature monitoring result attached to the Feature of a Feature Group.

        :param fm_result: feature monitoring result object to be attached to a Feature
        :type fm_result: `FeatureMonitoringResult`
        :return: the created feature monitoring result
        :rtype: FeatureMonitoringResult
        """
        _client = client.get_instance()
        path_params = self.build_path_params(
            project_id=_client._project_id,
        )

        headers = {"content-type": "application/json"}
        payload = fm_result.json()
        return FeatureMonitoringResult.from_response_json(
            _client._send_request("POST", path_params, headers=headers, data=payload)
        )

    def delete(
        self,
        result_id: int,
    ) -> None:
        """Delete the Feature Monitoring result attached to a Feature."""
        _client = client.get_instance()
        path_params = self.build_path_params(
            project_id=_client._project_id,
        )
        path_params.append(result_id)

        _client._send_request("DELETE", path_params)

    def get_by_config_id(
        self,
        config_id: int,
        query_params: Optional[Dict[str, Union[str, List[str]]]] = None,
    ) -> List[FeatureMonitoringResult]:
        """Get all Feature Monitoring Result attached to a Monitoring Config.

        :param config_id: Id of the feature monitoring config for which to fetch all results
        :type config_id: int
        :param query_params: query parameters to filter the results by monitoring_time
        :type query_params: Dict[str, Union[str, List[str]]], optional
        :return: fetched feature monitoring results attached to the Feature Group
        :rtype: List[FeatureMonitoringResult]
        """
        _client = client.get_instance()
        path_params = self.build_path_params(
            project_id=_client._project_id,
        )
        path_params.append("byconfig")
        path_params.append(config_id)
        headers = {"content-type": "application/json"}

        return FeatureMonitoringResult.from_response_json(
            _client._send_request("GET", path_params, query_params, headers=headers)
        )

    def get_by_id(
        self,
        result_id: int,
    ) -> List[FeatureMonitoringResult]:
        """Get the Feature Monitoring Result attached to a Feature.

        :param result_id: Id of the feature monitoring result to fetch
        :type result_id: int
        :return: fetched feature monitoring result attached to the Feature Group
        :rtype: FeatureMonitoringResult || None
        """
        _client = client.get_instance()
        path_params = self.build_path_params(
            project_id=_client._project_id,
        )
        path_params.append(result_id)

        return FeatureMonitoringResult.from_response_json(
            _client._send_request("GET", path_params)
        )

    def build_path_params(
        self,
        project_id: int,
    ) -> List[str]:
        """Build the path parameters for the Feature Monitoring Result API.

        :param project_id: Id of the project
        :type project_id: int
        :return: list of path parameters
        :rtype: List[str]
        """
        path_params = [
            "project",
            project_id,
            "featurestores",
            self._feature_store_id,
        ]
        if self._feature_group_id is not None:
            path_params.extend(["featuregroups", self._feature_group_id])
        else:
            path_params.extend(
                [
                    "featureview",
                    self._feature_view_name,
                    "version",
                    self._feature_view_version,
                ]
            )
        path_params.extend(["featuremonitoring", "result"])

        return path_params
