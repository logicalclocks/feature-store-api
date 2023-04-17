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
from typing import Optional, Mapping, Union
from hsfs.util import FeatureStoreEncoder, convert_event_time_to_timestamp
from datetime import datetime, date


class FeatureDescriptiveStatistics:
    _SINGLE_VALUE_STATISTICS = [
        "count",
        "completeness",
        "num_non_null_values",
        "num_null_values",
        "approx_num_distinct_values",
        "min",
        "max",
        "sum",
        "mean",
        "stddev",
        "distinctness",
        "entropy",
        "uniqueness",
        "exact_num_distinct_values",
    ]

    def __init__(
        self,
        feature_type: str,
        feature_name: str,
        count: int,
        end_time: Union[int, datetime, date, str],
        row_percentage: float,
        # for any feature type
        completeness: Optional[float] = None,
        num_non_null_values: Optional[int] = None,
        num_null_values: Optional[int] = None,
        approx_num_distinct_values: Optional[int] = None,
        # for numerical features
        min: Optional[float] = None,
        max: Optional[float] = None,
        sum: Optional[float] = None,
        mean: Optional[float] = None,
        stddev: Optional[float] = None,
        percentiles: Optional[Mapping[str, float]] = None,
        # with exact uniqueness
        distinctness: Optional[float] = None,
        entropy: Optional[float] = None,
        uniqueness: Optional[float] = None,
        exact_num_distinct_values: Optional[int] = None,
        # for filtering
        start_time: Optional[Union[int, datetime, date, str]] = None,
        id: Optional[int] = None,
    ):
        self._id = id
        self._feature_type = feature_type
        self._feature_name = feature_name
        self._count = count
        self._end_time = end_time
        self._row_percentage = row_percentage
        self._completeness = completeness
        self._num_non_null_values = num_non_null_values
        self._num_null_values = num_null_values
        self._approx_num_distinct_values = approx_num_distinct_values
        self._min = min
        self._max = max
        self._sum = sum
        self._mean = mean
        self._stddev = stddev
        self._percentiles = percentiles
        self._distinctness = distinctness
        self._entropy = entropy
        self._uniqueness = uniqueness
        self._exact_num_distinct_values = exact_num_distinct_values
        self._start_time = start_time

    def get_value(self, name):
        stat_name = name.lower()
        if stat_name not in self._SINGLE_VALUE_STATISTICS:
            return None
        try:
            return getattr(self, stat_name)
        except KeyError:
            raise AttributeError(f"'{name}' statistic has not been computed")

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    @classmethod
    def from_deequ_json(cls, json_dict):
        # TODO: to be removed after replacing deequ
        stats_dict = {
            "feature_type": json_dict["dataType"],
            "count": json_dict["numRecordsNull"] + json_dict["numRecordsNonNull"],
            "feature_name": json_dict["column"],
            "end_time": None,
            "row_percentage": None,
            # common for all data types
            "completeness": json_dict["completeness"],
            "num_non_null_values": json_dict["numRecordsNonNull"],
            "num_null_values": json_dict["numRecordsNull"],
            "approx_num_distinct_values": json_dict["approximateNumDistinctValues"],
        }
        if "uniqueness" in json_dict.keys():
            # commmon for all data types if exact_uniqueness is enabled
            stats_dict["uniqueness"] = json_dict["uniqueness"]
            stats_dict["entropy"] = json_dict["entropy"]
            stats_dict["distinctness"] = json_dict["distinctness"]
            stats_dict["exact_num_distinct_values"] = json_dict[
                "exactNumDistinctValues"
            ]
        if json_dict["dataType"] == "Fractional" or json_dict["dataType"] == "Integral":
            stats_dict["min"] = json_dict["minimum"]
            stats_dict["max"] = json_dict["maximum"]
            stats_dict["sum"] = json_dict["sum"]
            stats_dict["mean"] = json_dict["mean"]
            stats_dict["stddev"] = json_dict["stdDev"]

        return cls(**stats_dict)

    def to_dict(self):
        return {
            "id": self._id,
            "featureType": self._feature_type,
            "featureName": self._feature_name,
            "count": self._count,
            "min": self._min,
            "max": self._max,
            "sum": self._sum,
            "mean": self._mean,
            "stddev": self._stddev,
            "completeness": self._completeness,
            "numNonNullValues": self._num_non_null_values,
            "numNullValues": self._num_null_values,
            "distinctness": self._distinctness,
            "entropy": self._entropy,
            "uniqueness": self._uniqueness,
            "approxNumDistinctValues": self._approx_num_distinct_values,
            "exactNumDistinctValues": self._exact_num_distinct_values,
            "percentiles": self._percentiles,
            "startTime": self._start_time,
            "endTime": self._end_time,
            "rowPercentage": self._row_percentage,
        }

    def json(self) -> str:
        return json.dumps(self, cls=FeatureStoreEncoder)

    def __str__(self):
        return self.json()

    def __repr__(self) -> str:
        return json.dumps(humps.decamelize(self.to_dict()), indent=2)

    @property
    def id(self) -> Optional[int]:
        return self._id

    @property
    def feature_type(self) -> str:
        return self._feature_type

    @property
    def feature_name(self) -> str:
        return self._feature_name

    @property
    def count(self) -> int:
        return self._count

    @property
    def completeness(self) -> Optional[float]:
        return self._completeness

    @property
    def num_non_null_values(self) -> Optional[int]:
        return self._num_non_null_values

    @property
    def num_null_values(self) -> Optional[int]:
        return self._num_null_values

    @property
    def approx_num_distinct_values(self) -> Optional[int]:
        return self._approx_num_distinct_values

    @property
    def min(self) -> Optional[float]:
        return self._min

    @property
    def max(self) -> Optional[float]:
        return self._max

    @property
    def sum(self) -> Optional[float]:
        return self._sum

    @property
    def mean(self) -> Optional[float]:
        return self._mean

    @property
    def stddev(self) -> Optional[float]:
        return self._stddev

    @property
    def percentiles(self) -> Optional[Mapping[str, float]]:
        return self._percentiles

    @property
    def distinctness(self) -> Optional[float]:
        return self._distinctness

    @property
    def entropy(self) -> Optional[float]:
        return self._entropy

    @property
    def uniqueness(self) -> Optional[float]:
        return self._uniqueness

    @property
    def exact_num_distinct_values(self) -> Optional[int]:
        return self._exact_num_distinct_values

    @property
    def start_time(self) -> Optional[int]:
        return self._start_time

    @start_time.setter
    def start_time(self, start_time: Optional[Union[datetime, date, str, int]]):
        self._start_time = convert_event_time_to_timestamp(start_time)

    @property
    def end_time(self) -> int:
        return self._end_time

    @end_time.setter
    def end_time(self, end_time: Optional[Union[datetime, date, str, int]]):
        self._end_time = convert_event_time_to_timestamp(end_time)

    @property
    def row_percentage(self) -> float:
        return self._row_percentage

    @row_percentage.setter
    def row_percentage(self, row_percentage: float):
        if isinstance(row_percentage, int) or isinstance(row_percentage, float):
            row_percentage = float(row_percentage)
            if row_percentage <= 0.0 or row_percentage > 1.0:
                raise ValueError("Row percentage must be a float between 0 and 1.")
            self._row_percentage = row_percentage
        elif row_percentage is None:
            self._row_percentage = None
        else:
            raise TypeError("Row percentage must be a float between 0 and 1.")
