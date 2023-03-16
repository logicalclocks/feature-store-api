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
from typing import Optional, Mapping
from hsfs.util import FeatureStoreEncoder


class FeatureDescriptiveStatistics:
    def __init__(
        self,
        min: float,
        max: float,
        sum: float,
        count: int,
        mean: float,
        stddev: float,
        completeness: Optional[float] = None,
        num_non_null_values: Optional[int] = None,
        num_null_values: Optional[int] = None,
        distinctness: Optional[float] = None,
        entropy: Optional[float] = None,
        uniqueness: Optional[float] = None,
        approx_num_distinct_values: Optional[int] = None,
        exact_num_distinct_values: Optional[int] = None,
        percentiles: Optional[Mapping[str, float]] = None,
        id: Optional[int] = None,
    ):
        self._id = id
        self._min = min
        self._max = max
        self._sum = sum
        self._count = count
        self._mean = mean
        self._stddev = stddev
        self._completeness = completeness
        self._num_non_null_values = num_non_null_values
        self._num_null_values = num_null_values
        self._distinctness = distinctness
        self._entropy = entropy
        self._uniqueness = uniqueness
        self._approx_num_distinct_values = approx_num_distinct_values
        self._exact_num_distinct_values = exact_num_distinct_values
        self._percentiles = percentiles

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [cls(**result) for result in json_decamelized["items"]]
        else:
            return cls(**json_decamelized)

    @classmethod
    def from_deequ_json(cls, json_dict, feature_name: None):
        # TODO: to be removed after replacing deequ
        json_decamelized = humps.decamelize(json_dict)
        cols_stats = json_decamelized["columns"]
        stats = cols_stats[0]
        if len(cols_stats) > 0:
            if feature_name is None:
                raise ValueError("Feature name is missing")
            for col_stats in cols_stats:
                if col_stats["column"] == feature_name:
                    stats = col_stats
                    break

        # common for all data types
        stats_dict = {
            "completeness": stats["completeness"],
            "num_non_null_values": stats["numRecordsNonNull"],
            "num_null_values": stats["numRecordsNull"],
            "approx_num_distinct_values": stats["approximateNumDistinctValues"],
        }
        if stats["uniqueness"]:
            # commmon for all data types if exact_uniqueness is enabled
            stats_dict["uniqueness"] = stats["uniqueness"]
            stats_dict["entropy"] = stats["entropy"]
            stats_dict["distinctness"] = stats["distinctness"]
            stats_dict["exact_num_distinct_values"] = stats["exactNumDistinctValues"]
        if stats["dataType"] == "Fractional" or stats["dataType"] == "Integral":
            stats_dict["min"] = stats["minimum"]
            stats_dict["max"] = stats["maximum"]
            stats_dict["sum"] = stats["sum"]
            stats_dict["count"] = stats["numRecordsNull"] + stats["numRecordsNonNull"]
            stats_dict["mean"] = stats["mean"]
            stats_dict["stddev"] = stats["stdDev"]
            stats_dict["completeness"] = stats["completeness"]
            stats_dict["num_non_null_values"] = stats["numRecordsNonNull"]
            stats_dict["num_null_values"] = stats["numRecordsNull"]

        return cls(**stats_dict)

    def to_dict(self):
        return {
            "id": self._id,
            "min": self._min,
            "max": self._max,
            "sum": self._sum,
            "count": self._count,
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
    def min(self) -> float:
        return self._min

    @property
    def max(self) -> float:
        return self._max

    @property
    def sum(self) -> float:
        return self._sum

    @property
    def count(self) -> int:
        return self._count

    @property
    def mean(self) -> float:
        return self._mean

    @property
    def stddev(self) -> float:
        return self._stddev

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
    def distinctness(self) -> Optional[float]:
        return self._distinctness

    @property
    def entropy(self) -> Optional[float]:
        return self._entropy

    @property
    def uniqueness(self) -> Optional[float]:
        return self._uniqueness

    @property
    def approx_num_distinct_values(self) -> Optional[int]:
        return self._approx_num_distinct_values

    @property
    def exact_num_distinct_values(self) -> Optional[int]:
        return self._exact_num_distinct_values

    @property
    def percentiles(self) -> Optional[Mapping[str, float]]:
        return self._percentiles
