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

from typing import Optional
from hsfs import client
import feature_monitoring_config as fmc


class FeatureMonitoringConfigApi:
    def __init__(self, feature_store_id: int, feature_group_id: int):
        """Feature Monitoring Configuration endpoints for the Feature Group resource.

        :param feature_store_id: id of the respective Feature Store
        :type feature_store_id: int
        :param feature_group_id: id of the respective Feature Group
        :type feature_group_id: int
        """
        self._feature_store_id = feature_store_id
        self._feature_group_id = feature_group_id

    def create(
        self, fm_config: fmc.FeatureMonitoringConfig
    ) -> fmc.FeatureMonitoringConfig:
        """Create an feature monitoring configuration attached to the Feature of a Feature Group.

        :param fm_config: feature monitoring config object to be attached to a Feature
        :type fm_config: `FeatureMonitoringConfiguration`
        :return: the created feature monitoring configuration
        :rtype: FeatureMonitoringConfiguration
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
            "config",
        ]

        headers = {"content-type": "application/json"}
        payload = fm_config.json()
        return fmc.FeatureMonitoringConfig.from_response_json(
            _client._send_request("POST", path_params, headers=headers, data=payload)
        )

    def update(
        self, fm_config: fmc.FeatureMonitoringConfig
    ) -> fmc.FeatureMonitoringConfig:
        """Update a feature monitoring configuration attached to a Feature.

        :param fm_config: feature monitoring configuration to be attached to a Feature
        :type fm_config: `FeatureMonitoringConfig`
        :return: the updated feature monitoring configuration
        :rtype: FeatureMonitoringConfig
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
            "config",
            fm_config.id,
        ]

        headers = {"content-type": "application/json"}
        payload = fm_config.json()

        return fmc.FeatureMonitoringConfig.from_response_json(
            _client._send_request("PUT", path_params, headers=headers, data=payload)
        )

    def delete(self, config_id: int) -> None:
        """Delete the Feature Monitoring configuration attached to a Feature."""
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            self._feature_store_id,
            "featuregroups",
            self._feature_group_id,
            "featuremonitoring",
            "config",
            config_id,
        ]

        _client._send_request("DELETE", path_params)

    def get(self, config_id) -> Optional[fmc.FeatureMonitoringConfig]:
        """Get the Feature Monitoring Configuration attached to a Feature.

        :param config_id: Id of the feature monitoring configuration to fetch
        :type config_id: int
        :return: fetched feature monitoring configuration attached to the Feature Group
        :rtype: FeatureMonitoringConfig || None
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
            "config",
            config_id,
        ]

        return fmc.FeatureMonitoringConfig.from_response_json(
            _client._send_request("GET", path_params)
        )
