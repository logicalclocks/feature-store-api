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


import json
import humps
from typing import Any, Dict, List, Optional, Union
from hsfs import util


class FeatureMonitoringConfig:
    def __init__(
        self,
        feature_store_id: int,
        feature_name: str,
        name: str,
        feature_monitoring_type: str = "DESCRIPTIVE_STATISTICS",
        job_id: Optional[int] = None,
        detection_monitoring_window_configuration: Optional[Dict[Any, Any]] = None,
        reference_monitoring_window_configuration: Optional[Dict[Any, Any]] = None,
        descriptive_statistics_comparison_configuration: Optional[
            Dict[Any, Any]
        ] = None,
        alert_config: Optional[str] = None,
        scheduler_config: Optional[str] = None,
        enabled: bool = True,
        id: Optional[int] = None,
        feature_group_id: Optional[int] = None,
        feature_view_id: Optional[int] = None,
        description: Optional[str] = None,
        href: Optional[str] = None,
        items: Optional[List[Dict[str, Any]]] = None,
        count: Optional[int] = None,
    ):
        self._id = id
        self._href = href
        self._name = name
        self._description = description
        self._feature_store_id = feature_store_id
        self._feature_group_id = feature_group_id
        self._feature_view_id = feature_view_id
        self._feature_name = feature_name
        self._job_id = job_id
        self._feature_monitoring_type = feature_monitoring_type
        self._enabled = enabled
        self._scheduler_config = scheduler_config
        self._alert_config = alert_config
        self._descriptive_statistics_comparison_configuration = (
            descriptive_statistics_comparison_configuration
        )
        self._detection_monitoring_window_configuration = (
            detection_monitoring_window_configuration
        )
        self._reference_monitoring_window_configuration = (
            reference_monitoring_window_configuration
        )

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [cls(**config) for config in json_decamelized["items"]]
        else:
            return cls(**json_decamelized)

    def _window_config_to_dict(self, window_config: Dict[str, Any]) -> Dict[str, Any]:

        return {
            "windowConfigurationType": window_config.get(
                "window_configuration_type", "SPECIFIC_VALUE"
            ),
            "windowLength": window_config.get("window_length", None),
            "timeOffset": window_config.get("time_offset", None),
            "specificValue": window_config.get("specific_value", None),
            "specificId": window_config.get("specific_id", None),
            "rowPercentage": window_config.get("row_percentage", None),
            "id": window_config.get("id", None),
        }

    def to_dict(self):

        if isinstance(self._detection_monitoring_window_configuration, dict):
            detection_monitoring_window_configuration = self._window_config_to_dict(
                self._detection_monitoring_window_configuration
            )
        else:
            detection_monitoring_window_configuration = None

        if isinstance(self._reference_monitoring_window_configuration, dict):
            reference_monitoring_window_configuration = self._window_config_to_dict(
                self._reference_monitoring_window_configuration
            )
        else:
            reference_monitoring_window_configuration = None

        if isinstance(self._descriptive_statistics_comparison_configuration, dict):
            stats_comparison_config = {
                "threshold": self._descriptive_statistics_comparison_configuration.get(
                    "threshold", 0.0
                ),
                "compareOn": self._descriptive_statistics_comparison_configuration.get(
                    "compare_on", "MEAN"
                ),
                "strict": self._descriptive_statistics_comparison_configuration.get(
                    "strict", False
                ),
                "relative": self._descriptive_statistics_comparison_configuration.get(
                    "relative", False
                ),
            }
        else:
            stats_comparison_config = None

        return {
            "id": self._id,
            "featureStoreId": self._feature_store_id,
            "featureGroupId": self._feature_group_id,
            "featureViewId": self._feature_view_id,
            "featureName": self._feature_name,
            "enabled": self._enabled,
            "name": self._name,
            "description": self._description,
            "jobId": self._job_id,
            "featureMonitoringType": self._feature_monitoring_type,
            "schedulerConfig": self._scheduler_config,
            "alertConfig": self._alert_config,
            "detectionMonitoringWindowConfiguration": detection_monitoring_window_configuration,
            "referenceMonitoringWindowConfiguration": reference_monitoring_window_configuration,
            "descriptiveStatisticsComparisonConfiguration": stats_comparison_config,
        }

    def json(self) -> str:
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def __str__(self):
        return self.json()

    @property
    def id(self) -> Optional[int]:
        return self._id

    @property
    def feature_store_id(self) -> int:
        return self._feature_store_id

    @property
    def feature_group_id(self) -> Optional[int]:
        return self._feature_group_id

    @property
    def feature_view_id(self) -> Optional[int]:
        return self._feature_view_id

    @property
    def feature_name(self) -> str:
        return self._feature_name

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> Optional[str]:
        return self._description

    @property
    def job_id(self) -> Optional[int]:
        return self._job_id

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def feature_monitoring_type(self) -> Optional[str]:
        return self._feature_monitoring_type

    @property
    def alert_config(self) -> Optional[str]:
        return self._alert_config

    @property
    def scheduler_config(self) -> Optional[str]:
        return self._scheduler_config

    @property
    def detection_monitoring_window_configuration(self) -> Dict[str, Any]:
        return self._detection_monitoring_window_configuration

    @detection_monitoring_window_configuration.setter
    def detection_monitoring_window_configuration(
        self, detection_monitoring_window_configuration: Union[Dict[str, Any], str]
    ):
        if isinstance(detection_monitoring_window_configuration, str):
            self._detection_monitoring_window_configuration = json.loads(
                detection_monitoring_window_configuration
            )
        else:
            self._detection_monitoring_window_configuration = (
                detection_monitoring_window_configuration
            )

    @property
    def reference_window_monitoring_configuration(self) -> Optional[Dict[str, Any]]:
        return self._reference_monitoring_window_configuration

    @reference_window_monitoring_configuration.setter
    def reference_window_monitoring_configuration(
        self, reference_monitoring_window_configuration: Union[Dict[str, Any], str]
    ):
        if isinstance(reference_monitoring_window_configuration, str):
            self._reference_monitoring_window_configuration = json.loads(
                reference_monitoring_window_configuration
            )
        else:
            self._reference_monitoring_window_configuration = (
                reference_monitoring_window_configuration
            )

    @property
    def descriptive_statistics_comparison_configuration(
        self,
    ) -> Optional[Dict[str, Any]]:
        return self._descriptive_statistics_comparison_configuration

    @descriptive_statistics_comparison_configuration.setter
    def descriptive_statistics_comparison_configuration(
        self,
        descriptive_statistics_comparison_configuration: Union[Dict[str, Any], str],
    ):
        if isinstance(descriptive_statistics_comparison_configuration, str):
            self._descriptive_statistics_comparison_configuration = json.loads(
                descriptive_statistics_comparison_configuration
            )
        else:
            self._descriptive_statistics_comparison_configuration = (
                descriptive_statistics_comparison_configuration
            )
