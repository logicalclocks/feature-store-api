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

import json
import humps
from typing import Optional, Union
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
        feature_name: str,
        difference: Optional[float] = None,
        shift_detected: bool = False,
        detection_statistics_id: Optional[int] = None,
        reference_statistics_id: Optional[int] = None,
        empty_detection_window: bool = False,
        empty_reference_window: bool = False,
        specific_value: Optional[float] = None,
        raised_exception: bool = False,
        detection_statistics: Optional[
            Union[FeatureDescriptiveStatistics, dict]
        ] = None,
        reference_statistics: Optional[
            Union[FeatureDescriptiveStatistics, dict]
        ] = None,
        id: Optional[int] = None,
        href: Optional[str] = None,
        **kwargs,
    ):
        self._id = id
        self._href = href
        self._feature_store_id = feature_store_id
        self._execution_id = execution_id
        self._config_id = config_id
        self._feature_name = feature_name
        self._detection_statistics_id = detection_statistics_id
        self._reference_statistics_id = reference_statistics_id
        self._detection_statistics = self._parse_descriptive_statistics(
            detection_statistics
        )
        self._reference_statistics = self._parse_descriptive_statistics(
            reference_statistics
        )
        self._monitoring_time = util.convert_event_time_to_timestamp(monitoring_time)
        self._difference = difference
        self._shift_detected = shift_detected
        self._empty_detection_window = empty_detection_window
        self._empty_reference_window = empty_reference_window
        self._raised_exception = raised_exception
        self._specific_value = specific_value

    def _parse_descriptive_statistics(
        self,
        statistics: Optional[Union[FeatureDescriptiveStatistics, dict]],
    ) -> Optional[FeatureDescriptiveStatistics]:
        if statistics is None:
            return None
        return (
            statistics
            if isinstance(statistics, FeatureDescriptiveStatistics)
            else FeatureDescriptiveStatistics.from_response_json(statistics)
        )

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
        the_dict = {
            "id": self._id,
            "featureStoreId": self._feature_store_id,
            "configId": self._config_id,
            "executionId": self._execution_id,
            "monitoringTime": self._monitoring_time,
            "difference": self._difference,
            "shiftDetected": self._shift_detected,
            "featureName": self._feature_name,
            "emptyDetectionWindow": self._empty_detection_window,
            "emptyReferenceWindow": self._empty_reference_window,
            "raisedException": self._raised_exception,
            "specificValue": self._specific_value,
        }

        if self._detection_statistics_id is not None:
            the_dict["detectionStatisticsId"] = self._detection_statistics_id
        if self._reference_statistics_id is not None:
            the_dict["referenceStatisticsId"] = self._reference_statistics_id
        if self._detection_statistics is not None:
            the_dict["detectionStatistics"] = self._detection_statistics.to_dict()
        if self._reference_statistics is not None:
            the_dict["referenceStatistics"] = self._reference_statistics.to_dict()

        return the_dict

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
    def detection_statistics_id(self) -> Optional[int]:
        return self._detection_statistics_id

    @property
    def reference_statistics_id(self) -> Optional[int]:
        return self._reference_statistics_id

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
    def difference(self) -> Optional[float]:
        return self._difference

    @property
    def shift_detected(self) -> bool:
        return self._shift_detected

    @property
    def feature_name(self) -> str:
        return self._feature_name

    @property
    def empty_detection_window(self) -> bool:
        return self._empty_detection_window

    @property
    def empty_reference_window(self) -> bool:
        return self._empty_reference_window

    @property
    def specific_value(self) -> Optional[float]:
        return self._specific_value
