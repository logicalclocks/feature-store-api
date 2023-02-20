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

from typing import List, Optional
from hsfs import client
import hsfs.core.feature_monitoring_config as fmc


class FeatureMonitoringConfigApi:
    def __init__(self, feature_store_id: int):
        """Feature Monitoring Configuration endpoints for the Feature Group resource.

        :param feature_store_id: id of the respective Feature Store
        :type feature_store_id: int
        """
        self._feature_store_id = feature_store_id

    def create(
        self,
        fm_config: fmc.FeatureMonitoringConfig,
        feature_group_id: Optional[int] = None,
        feature_view_name: Optional[str] = None,
        feature_view_version: Optional[int] = None,
    ) -> fmc.FeatureMonitoringConfig:
        """Create an feature monitoring configuration attached to the Feature of a Feature Group.

        :param fm_config: feature monitoring config object to be attached to a Feature
        :type fm_config: `FeatureMonitoringConfiguration`
        :return: the created feature monitoring configuration
        :rtype: FeatureMonitoringConfiguration
        """
        _client = client.get_instance()
        path_params = self.build_path_params(
            project_id=_client._project_id,
            feature_group_id=feature_group_id,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
        )

        headers = {"content-type": "application/json"}
        payload = fm_config.json()
        return fmc.FeatureMonitoringConfig.from_response_json(
            _client._send_request("POST", path_params, headers=headers, data=payload)
        )

    def update(
        self,
        fm_config: fmc.FeatureMonitoringConfig,
        feature_group_id: Optional[int] = None,
        feature_view_name: Optional[str] = None,
        feature_view_version: Optional[int] = None,
    ) -> fmc.FeatureMonitoringConfig:
        """Update a feature monitoring configuration attached to a Feature.

        :param fm_config: feature monitoring configuration to be attached to a Feature
        :type fm_config: `FeatureMonitoringConfig`
        :return: the updated feature monitoring configuration
        :rtype: FeatureMonitoringConfig
        """
        _client = client.get_instance()
        path_params = self.build_path_params(
            project_id=_client._project_id,
            feature_group_id=feature_group_id,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
            config_id=fm_config._id,
        )

        headers = {"content-type": "application/json"}
        payload = fm_config.json()

        return fmc.FeatureMonitoringConfig.from_response_json(
            _client._send_request("PUT", path_params, headers=headers, data=payload)
        )

    def delete(
        self,
        config_id: int,
        feature_group_id: Optional[int] = None,
        feature_view_name: Optional[str] = None,
        feature_view_version: Optional[int] = None,
    ) -> None:
        """Delete the Feature Monitoring configuration attached to a Feature."""
        _client = client.get_instance()
        path_params = self.build_path_params(
            project_id=_client._project_id,
            feature_group_id=feature_group_id,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
            config_id=config_id,
        )

        _client._send_request("DELETE", path_params)

    def get(
        self,
        config_id: int,
        feature_group_id: Optional[int] = None,
        feature_view_name: Optional[str] = None,
        feature_view_version: Optional[int] = None,
    ) -> Optional[fmc.FeatureMonitoringConfig]:
        """Get the Feature Monitoring Configuration attached to a Feature.

        :param config_id: Id of the feature monitoring configuration to fetch
        :type config_id: int
        :return: fetched feature monitoring configuration attached to the Feature Group
        :rtype: FeatureMonitoringConfig || None
        """
        _client = client.get_instance()
        path_params = self.build_path_params(
            project_id=_client._project_id,
            feature_group_id=feature_group_id,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
            config_id=config_id,
        )

        return fmc.FeatureMonitoringConfig.from_response_json(
            _client._send_request("GET", path_params)
        )

    def build_path_params(
        self,
        project_id: int,
        feature_group_id: Optional[int] = None,
        feature_view_name: Optional[str] = None,
        feature_view_version: Optional[int] = None,
        config_id: Optional[int] = None,
    ) -> List[str]:
        path_params = [
            "project",
            project_id,
            "featurestores",
            self._feature_store_id,
        ]
        if feature_group_id is not None:
            path_params.extend(["featuregroups", feature_group_id])
        else:
            path_params.extend(
                ["featureview", feature_view_name, "version", feature_view_version]
            )
        path_params.extend(["featuremonitoring", "config", config_id])

        return path_params