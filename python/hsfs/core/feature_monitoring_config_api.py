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

from typing import List, Optional
from hsfs import client
from hsfs.core import feature_monitoring_config as fmc
from hsfs.core.job import Job


class FeatureMonitoringConfigApi:
    """Feature Monitoring Configuration endpoints for the Feature Group resource.
    :param feature_store_id: id of the respective Feature Store
    :type feature_store_id: int
    :param feature_group_id: id of the feature group, if monitoring a feature group
    :type feature_group_id: int, optional
    :param feature_view_name: name of the feature view, if monitoring a feature view
    :type feature_view_name: str, optional
    :param feature_view_version: version of the feature view, if monitoring a feature view
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
        fm_config: "fmc.FeatureMonitoringConfig",
    ) -> "fmc.FeatureMonitoringConfig":
        """Create an feature monitoring configuration attached to the Feature of a Feature Group.
        :param fm_config: feature monitoring config object to be attached to a Feature
        :type fm_config: `FeatureMonitoringConfiguration`
        :return: the created feature monitoring configuration
        :rtype: FeatureMonitoringConfiguration
        """
        _client = client.get_instance()
        path_params = self.build_path_params(
            project_id=_client._project_id,
        )

        headers = {"content-type": "application/json"}
        payload = fm_config.json()
        return fmc.FeatureMonitoringConfig.from_response_json(
            _client._send_request("POST", path_params, headers=headers, data=payload)
        )

    def update(
        self,
        fm_config: "fmc.FeatureMonitoringConfig",
    ) -> "fmc.FeatureMonitoringConfig":
        """Update a feature monitoring configuration attached to a Feature.
        :param fm_config: feature monitoring configuration to be attached to a Feature
        :type fm_config: `FeatureMonitoringConfig`
        :return: the updated feature monitoring configuration
        :rtype: FeatureMonitoringConfig
        """
        _client = client.get_instance()
        path_params = self.build_path_params(
            project_id=_client._project_id,
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
    ) -> None:
        """Delete the Feature Monitoring configuration attached to a Feature.
        :param config_id: Id of the feature monitoring configuration to delete
        :type config_id: int
        :return: fetched feature monitoring configuration attached to the Feature Group
        :rtype: FeatureMonitoringConfig || None
        """
        _client = client.get_instance()
        path_params = self.build_path_params(
            project_id=_client._project_id,
            config_id=config_id,
        )

        _client._send_request("DELETE", path_params)

    def get_by_id(
        self,
        config_id: int,
    ) -> Optional["fmc.FeatureMonitoringConfig"]:
        """Get the Feature Monitoring Configuration attached to a Feature.
        :param config_id: Id of the feature monitoring configuration to fetch
        :type config_id: int
        :return: fetched feature monitoring configuration attached to the Feature Group
        :rtype: FeatureMonitoringConfig || None
        """
        _client = client.get_instance()
        path_params = self.build_path_params(
            project_id=_client._project_id,
            config_id=config_id,
        )

        return fmc.FeatureMonitoringConfig.from_response_json(
            _client._send_request("GET", path_params)
        )

    def get_by_feature_name(
        self,
        feature_name: str,
    ) -> Optional["fmc.FeatureMonitoringConfig"]:
        """Get all Feature Monitoring Configurations attached to a Feature Name.
        :param feature_name: Name of the feature for which to fetch monitoring configuration
        :type feature_name: str
        :return: fetched feature monitoring configuration attached to the Feature Group
        :rtype: List[FeatureMonitoringConfig] || None
        """
        _client = client.get_instance()
        path_params = self.build_path_params(
            project_id=_client._project_id,
            feature_name=feature_name,
        )

        return fmc.FeatureMonitoringConfig.from_response_json(
            _client._send_request("GET", path_params)
        )

    def get_by_name(
        self,
        name: str,
    ) -> Optional["fmc.FeatureMonitoringConfig"]:
        """Get all Feature Monitoring Configurations attached to a Feature Name.
        :param name: Name of the feature monitoring configuration to fetch
        :type name: str
        :return: fetched feature monitoring configuration attached to the Feature Group
        :rtype: FeatureMonitoringConfig || None
        """
        _client = client.get_instance()
        path_params = self.build_path_params(
            project_id=_client._project_id,
            name=name,
        )

        return fmc.FeatureMonitoringConfig.from_response_json(
            _client._send_request("GET", path_params)
        )

    def get_by_entity(self) -> List["fmc.FeatureMonitoringConfig"]:
        """Get all Feature Monitoring Configurations attached to a Feature Name.
        :param name: Name of the feature monitoring configuration to fetch
        :type name: str
        :return: fetched feature monitoring configuration attached to the Feature Group
        :rtype: List[FeatureMonitoringConfig] || None
        """
        _client = client.get_instance()
        path_params = self.build_path_params(
            project_id=_client._project_id,
            entity=True,
        )

        return fmc.FeatureMonitoringConfig.from_response_json(
            _client._send_request("GET", path_params)
        )

    def setup_feature_monitoring_job(
        self,
        config_name: str,
    ) -> Job:
        """Setup a feature monitoring job for a configuration.
        :param config_name: Name of the feature monitoring configuration to setup a job for
        :type config_name: str
        :return: Job object for the feature monitoring job
        :rtype: Job
        """
        _client = client.get_instance()
        path_params = self.build_path_params(
            project_id=_client._project_id,
        )
        path_params.extend(["setup", config_name])

        return Job.from_response_json(_client._send_request("POST", path_params))

    def trigger_feature_monitoring_job(
        self,
        config_id: int,
    ) -> Job:
        """Trigger a feature monitoring job for a configuration.
        :param config_id: Id of the feature monitoring configuration to trigger a job for
        :type config_id: int
        :return: Job attached to the monitoring configuration
        :rtype: Job
        """
        _client = client.get_instance()
        path_params = self.build_path_params(
            project_id=_client._project_id,
            config_id=config_id,
        )
        path_params.append("trigger")

        return Job.from_response_json(_client._send_request("POST", path_params))

    def build_path_params(
        self,
        project_id: int,
        feature_name: Optional[str] = None,
        config_id: Optional[int] = None,
        name: Optional[str] = None,
        entity: Optional[bool] = False,
    ) -> List[str]:
        """Builds the path parameters for the Feature Monitoring Config API.
        :param project_id: Id of the project
        :type project_id: int
        :type feature_name: str, optional
        :param config_id: Id of the feature monitoring configuration.
            Only to fetch feature monitoring configuration by id.
        :type config_id: int, optional
        :param name: Name of the feature monitoring configuration.
            Only to fetch feature monitoring configuration by name.
        :type name: str, optional
        :param entity: Whether to append entity to path params. Defaults to False.
            Only to fetch all feature monitoring configurations attached to an entity.
        :return: path parameters
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
        path_params.extend(["featuremonitoring", "config"])

        if config_id:
            path_params.append(config_id)
        elif feature_name:
            path_params.extend(["feature", feature_name])
        elif name:
            path_params.extend(["name", name])
        elif entity:
            path_params.append("entity")

        return path_params
