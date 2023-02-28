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
from typing import Any, Dict, List, Optional
from hsfs import util


class FeatureMonitoringConfig:
    def __init__(
        self,
        feature_store_id: int,
        feature_name: str,
        name: str,
        feature_monitoring_type: str = "DESCRIPTIVE_STATISTICS",
        job_id: Optional[int] = None,
        detection_window_config: Optional[Dict[str, Any]] = None,
        reference_window_config: Optional[Dict[str, Any]] = None,
        statistics_comparison_config: Optional[Dict[str, Any]] = None,
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
        self._statistics_comparison_config = statistics_comparison_config
        self._detection_window_config = detection_window_config
        self._reference_window_config = reference_window_config

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
            "windowConfigType": window_config.get(
                "window_config_type", "SPECIFIC_VALUE"
            ),
            "windowLength": window_config.get("window_length", None),
            "timeOffset": window_config.get("time_offset", None),
            "specificValue": window_config.get("specific_value", None),
            "specificId": window_config.get("specific_id", None),
            "rowPercentage": window_config.get("row_percentage", None),
            "id": window_config.get("id", None),
        }

    def to_dict(self):

        if isinstance(self._detection_window_config, dict):
            detection_window_config = self._window_config_to_dict(
                self._detection_window_config
            )
        else:
            detection_window_config = None

        if isinstance(self._reference_window_config, dict):
            reference_window_config = self._window_config_to_dict(
                self._reference_window_config
            )
        else:
            reference_window_config = None

        if isinstance(self._statistics_comparison_config, dict):
            statistics_comparison_config = {
                "threshold": self._statistics_comparison_config.get("threshold", 0.0),
                "compareOn": self._statistics_comparison_config.get(
                    "compare_on", "MEAN"
                ),
                "strict": self._statistics_comparison_config.get("strict", False),
                "relative": self._statistics_comparison_config.get("relative", False),
            }
        else:
            statistics_comparison_config = None

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
            "detectionWindowConfig": detection_window_config,
            "referenceWindowConfig": reference_window_config,
            "statisticsComparisonConfig": statistics_comparison_config,
        }

    def json(self) -> str:
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return humps.decamelize(self.to_dict())

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
    def detection_window_config(self) -> Dict[str, Any]:
        return self._detection_window_config

    @property
    def reference_window_config(self) -> Optional[Dict[str, Any]]:
        return self._reference_window_config

    @property
    def statistics_comparison_config(
        self,
    ) -> Optional[Dict[str, Any]]:
        return self._statistics_comparison_config
