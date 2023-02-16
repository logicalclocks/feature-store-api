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
from typing import Optional
from hsfs import util


class FeatureMonitoringResult:
    def __init__(
        self,
        feature_store_id: int,
        entity_id: int,
        execution_id: int,
        job_id: int,
        detection_stats_id: int,
        monitoring_time: int,
        feature_monitoring_config_id: int,
        difference: Optional[float] = None,
        triggered_alert: bool = False,
        reference_stats_id: Optional[int] = None,
        id: Optional[int] = None,
        href: Optional[str] = None,
    ):
        self._id = id
        self._href = href
        self._feature_store_id = feature_store_id
        self._entity_id = entity_id
        self._execution_id = execution_id
        self._job_id = job_id
        self._detection_stats_id = detection_stats_id
        self._reference_stats_id = reference_stats_id
        self._feature_monitoring_config_id = feature_monitoring_config_id

        self._monitoring_time = monitoring_time
        self._difference = difference
        self._triggered_alert = triggered_alert

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [
                cls(**expectation_suite)
                for expectation_suite in json_decamelized["items"]
            ]
        else:
            return cls(**json_decamelized)

    def to_dict(self):

        return {
            "id": self._id,
            "feature_store_id": self._feature_store_id,
            "entity_id": self._entity_id,
            "feature_monitoring_config_id": self._feature_monitoring_config_id,
            "job_id": self._job_id,
            "execution_id": self._execution_id,
            "detection_stats_id": self._detection_stats_id,
            "reference_stats_id": self._reference_stats_id,
            "monitoring_time": self._monitoring_time,
            "difference": self._difference,
            "triggered_alert": self._triggered_alert,
        }

    def json(self) -> str:
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def __str__(self):
        return self.json()
