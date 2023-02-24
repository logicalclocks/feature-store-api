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
        name: str,
        feature_name: str,
        detection_window_config: Dict[str, Any],
        scheduler_config: Optional[str] = None,
        feature_group_id: Optional[int] = None,
        feature_view_id: Optional[int] = None,
        feature_view_name: Optional[str] = None,
        feature_view_version: Optional[str] = None,
        description: Optional[str] = None,
    ) -> FeatureMonitoringConfig:

        config = self.build_stats_monitoring_only_config(
            name=name,
            feature_name=feature_name,
            detection_window_config=detection_window_config,
            scheduler_config=scheduler_config,
            feature_group_id=feature_group_id,
            feature_view_id=feature_view_id,
            description=description,
        )

        config._job_id = self.setup_monitoring_job(config)

        return self._feature_monitoring_config_api.create(
            fm_config=config,
            feature_group_id=feature_group_id,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
        )

    def enable_feature_monitoring_config(
        self,
        feature_name: str,
        name: str,
        detection_window_config: Dict[str, Any],
        reference_window_config: Dict[str, Any],
        statistics_comparison_config: Dict[str, Any],
        alert_config: str,
        scheduler_config: str,
        description: Optional[str] = None,
        feature_group_id: Optional[int] = None,
        feature_view_id: Optional[int] = None,
        feature_view_name: Optional[str] = None,
        feature_view_version: Optional[str] = None,
    ) -> FeatureMonitoringConfig:

        config = self.build_feature_monitoring_config(
            name=name,
            feature_name=feature_name,
            feature_group_id=feature_group_id,
            feature_view_id=feature_view_id,
            detection_window_config=detection_window_config,
            reference_window_config=reference_window_config,
            statistics_comparison_config=statistics_comparison_config,
            scheduler_config=scheduler_config,
            alert_config=alert_config,
            description=description,
        )

        config._job_id = self.setup_monitoring_job(config)

        return self._feature_monitoring_config_api.create(
            fm_config=config,
            feature_group_id=feature_group_id,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
        )

    def build_monitoring_window_config(
        self,
        window_config_type: str,
        time_offset: Optional[str] = None,
        window_length: Optional[str] = None,
        specific_value: Optional[float] = None,
        specific_id: Optional[int] = None,
        row_percentage: Optional[int] = None,
    ) -> Dict[str, Any]:

        return {
            "window_config_type": window_config_type,
            "time_offset": time_offset,
            "window_length": window_length,
            "specific_id": specific_id,
            "specific_value": specific_value,
            "row_percentage": row_percentage,
        }

    def build_stats_monitoring_only_config(
        self,
        name: str,
        feature_name: str,
        detection_window_config: Dict[str, Any],
        scheduler_config: Dict[str, Any],
        feature_group_id: Optional[int] = None,
        feature_view_id: Optional[int] = None,
        description: Optional[str] = None,
    ) -> FeatureMonitoringConfig:

        return FeatureMonitoringConfig(
            feature_store_id=self._feature_store_id,
            feature_group_id=feature_group_id,
            feature_view_id=feature_view_id,
            feature_name=feature_name,
            name=name,
            description=description,
            feature_monitoring_type="DESCRIPTIVE_STATISTICS",
            detection_window_config=detection_window_config,
            scheduler_config=scheduler_config,
            enabled=True,
            alert_config=None,
            reference_window_config=None,
            statistics_comparison_config=None,
        )

    def build_feature_monitoring_config(
        self,
        feature_name: str,
        name: str,
        detection_window_config: Dict[str, Any],
        reference_window_config: Dict[str, Any],
        statistics_comparison_config: Dict[str, Any],
        scheduler_config: str,
        alert_config: str,
        feature_group_id: Optional[int] = None,
        feature_view_id: Optional[int] = None,
        description: Optional[str] = None,
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
            name=name,
            description=description,
            alert_config=alert_config,
            reference_window_config=reference_window_config,
            statistics_comparison_config=statistics_comparison_config,
        )

    def setup_monitoring_job(self, _config: FeatureMonitoringConfig) -> int:
        return 1
