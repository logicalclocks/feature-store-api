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
        detection_window_config: Optional[Dict[Any, Any]] = None,
        reference_window_config: Optional[Dict[Any, Any]] = None,
        descriptive_statistics_comparison_config: Optional[Dict[Any, Any]] = None,
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
        self._descriptive_statistics_comparison_config = (
            descriptive_statistics_comparison_config
        )
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

    def to_dict(self):

        return {
            "id": self._id,
            "feature_store_id": self._feature_store_id,
            "feature_group_id": self._feature_group_id,
            "feature_view_id": self._feature_view_id,
            "feature_name": self._feature_name,
            "enabled": self._enabled,
            "name": self._name,
            "description": self._description,
            "job_id": self._job_id,
            "feature_monitoring_type": self._feature_monitoring_type,
            "scheduler_config_DTO": self._scheduler_config,
            "alert_config_DTO": self._alert_config,
            "detection_monitoring_window_configuration_DTO": self._detection_window_config,
            "reference_monitoring_window_configuration_DTO": self._reference_window_config,
            "descriptive_statistics_comparison_configuration_DTO": self._descriptive_statistics_comparison_config,
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
    def detection_window_config(self) -> Dict[str, Any]:
        return self._detection_window_config

    @property
    def reference_window_config(self) -> Optional[Dict[str, Any]]:
        return self._reference_window_config

    @property
    def descriptive_statistics_comparison_config(self) -> Optional[Dict[str, Any]]:
        return self._descriptive_statistics_comparison_config
