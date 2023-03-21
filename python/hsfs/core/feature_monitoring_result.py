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
from datetime import datetime, date

from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics


class FeatureMonitoringResult:
    def __init__(
        self,
        feature_store_id: int,
        execution_id: int,
        monitoring_time: Union[int, datetime, date, str],
        config_id: int,
        difference: Optional[float] = None,
        shift_detected: bool = False,
        detection_stats_id: Optional[int] = None,
        reference_stats_id: Optional[int] = None,
        detection_statistics: Optional[FeatureDescriptiveStatistics] = None,
        reference_statistics: Optional[FeatureDescriptiveStatistics] = None,
        id: Optional[int] = None,
        href: Optional[str] = None,
        items: Optional[List[Dict[str, Any]]] = None,
        count: Optional[int] = None,
    ):
        self._id = id
        self._href = href
        self._feature_store_id = feature_store_id
        self._execution_id = execution_id
        self._config_id = config_id

        self._detection_stats_id = detection_stats_id
        self._reference_stats_id = reference_stats_id
        self._detection_statistics = detection_statistics
        self._reference_statistics = reference_statistics

        self._monitoring_time = util.convert_event_time_to_timestamp(monitoring_time)
        self._difference = difference
        self._shift_detected = shift_detected

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [cls(**result) for result in json_decamelized["items"]]
        else:
            return cls(**json_decamelized)

    def to_dict(self):
        result_json = {
            "id": self._id,
            "featureStoreId": self._feature_store_id,
            "configId": self._config_id,
            "executionId": self._execution_id,
            "detectionStatsId": self._detection_stats_id,
            "referenceStatsId": self._reference_stats_id,
            "monitoringTime": self._monitoring_time,
            "difference": self._difference,
            "shiftDetected": self._shift_detected,
        }
        if self._detection_statistics is not None:
            del result_json["detectionStatsId"]
            result_json["detectionStatistics"] = self._detection_statistics.to_dict()
        if self._reference_statistics is not None:
            del result_json["referenceStatsId"]
            result_json["referenceStatistics"] = self._reference_statistics.to_dict()

        return result_json

    def json(self) -> str:
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def __str__(self):
        return self.json()

    def __repr__(self) -> str:
        return json.dumps(humps.decamelize(self.to_dict()), indent=2)

    @property
    def id(self) -> Optional[int]:
        return self._id

    @property
    def config_id(self) -> int:
        return self._config_id

    @property
    def feature_store_id(self) -> int:
        return self._feature_store_id

    @property
    def detection_stats_id(self) -> Optional[int]:
        return self._detection_stats_id

    @property
    def reference_stats_id(self) -> Optional[int]:
        return self._reference_stats_id

    @property
    def detection_statistics(self) -> Optional[FeatureDescriptiveStatistics]:
        return self._detection_statistics

    @property
    def reference_statistics(self) -> Optional[FeatureDescriptiveStatistics]:
        return self._reference_statistics

    @property
    def execution_id(self) -> Optional[int]:
        return self._execution_id

    @property
    def monitoring_time(self) -> int:
        return self._monitoring_time

    @property
    def difference(self) -> float:
        return self._difference

    @property
    def shift_detected(self) -> bool:
        return self._shift_detected
