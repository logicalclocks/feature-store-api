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

from typing import Any, Dict, Optional
from hsfs.core.feature_monitoring_config import FeatureMonitoringConfig
from hsfs.core.feature_monitoring_config_api import FeatureMonitoringConfigApi


class FeatureMonitoringConfigEngine:
    def __init__(self, feature_store_id: int) -> None:
        self._feature_store_id = feature_store_id

        self._feature_monitoring_config_api = FeatureMonitoringConfigApi(
            feature_store_id=feature_store_id
        )

    def enable_descriptive_statistics_monitoring(
        self,
        feature_name: str,
        detection_window_config: Dict[str, Any],
        scheduler_config: str,
        feature_group_id: Optional[int] = None,
        feature_view_id: Optional[int] = None,
        feature_view_name: Optional[str] = None,
        feature_view_version: Optional[str] = None,
    ) -> FeatureMonitoringConfig:

        config = self.build_stats_monitoring_only_config(
            feature_name=feature_name,
            detection_window_config=detection_window_config,
            scheduler_config=scheduler_config,
            feature_group_id=feature_group_id,
            feature_view_id=feature_view_id,
        )

        return self._feature_monitoring_config_api.create(
            fm_config=config,
            feature_group_id=feature_group_id,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
        )

    def enable_feature_monitoring_config(
        self,
        feature_name: str,
        detection_window_config: Dict[str, Any],
        reference_window_config: Dict[str, Any],
        descriptive_statistics_monitoring_config: Dict[str, Any],
        alert_config: str,
        scheduler_config: str,
        feature_group_id: Optional[int] = None,
        feature_view_id: Optional[int] = None,
        feature_view_name: Optional[str] = None,
        feature_view_version: Optional[str] = None,
    ) -> FeatureMonitoringConfig:

        config = self.build_feature_monitoring_config(
            feature_name=feature_name,
            feature_group_id=feature_group_id,
            feature_view_id=feature_view_id,
            detection_window_config=detection_window_config,
            reference_window_config=reference_window_config,
            descriptive_statistics_monitoring_config=descriptive_statistics_monitoring_config,
            scheduler_config=scheduler_config,
            alert_config=alert_config,
        )

        return self._feature_monitoring_config_api.create(
            fm_config=config,
            feature_group_id=feature_group_id,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
        )

    def build_monitoring_window_config(
        self,
        entity_to_monitor: str,
        time_offset: str,
        window_length: str,
    ) -> Dict[str, Any]:

        return {
            "window_builder_type": entity_to_monitor,
            "time_offset": time_offset,
            "window_length": window_length,
        }

    def build_stats_monitoring_only_config(
        self,
        feature_name: str,
        detection_window_config: Dict[str, Any],
        scheduler_config: Dict[str, Any],
        feature_group_id: Optional[int] = None,
        feature_view_id: Optional[int] = None,
    ) -> FeatureMonitoringConfig:

        return FeatureMonitoringConfig(
            feature_store_id=self._feature_store_id,
            feature_group_id=feature_group_id,
            feature_view_id=feature_view_id,
            feature_name=feature_name,
            feature_monitoring_type="DESCRIPTIVE_STATISTICS",
            detection_window_config=detection_window_config,
            scheduler_config=scheduler_config,
            enabled=True,
            alert_config=None,
            reference_window_config=None,
            descriptive_statistics_monitoring_config=None,
        )

    def build_feature_monitoring_config(
        self,
        feature_name: str,
        detection_window_config: Dict[str, Any],
        reference_window_config: Dict[str, Any],
        descriptive_statistics_monitoring_config: Dict[str, Any],
        scheduler_config: str,
        alert_config: str,
        feature_group_id: Optional[int] = None,
        feature_view_id: Optional[int] = None,
    ) -> FeatureMonitoringConfig:

        return FeatureMonitoringConfig(
            feature_store_id=self._feature_store_id,
            feature_group_id=feature_group_id,
            feature_view_id=feature_view_id,
            feature_name=feature_name,
            feature_monitoring_type="DESCRIPTIVE_STATISTICS",
            detection_window_config=detection_window_config,
            scheduler_config=scheduler_config,
            enabled=True,
            alert_config=alert_config,
            reference_window_config=reference_window_config,
            descriptive_statistics_monitoring_config=descriptive_statistics_monitoring_config,
        )
