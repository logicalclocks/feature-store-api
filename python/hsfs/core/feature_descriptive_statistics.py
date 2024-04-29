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
from __future__ import annotations

import json
from typing import Mapping, Optional, Union

import humps
from hsfs import util


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
        feature_name: str,
        feature_type: str = None,
        count: int = None,
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
        extended_statistics: Optional[Union[dict, str]] = None,
        id: Optional[int] = None,
        **kwargs,
    ):
        self._id = id
        self._feature_type = feature_type
        self._feature_name = util.autofix_feature_name(feature_name)
        self._count = count
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
        self._extended_statistics = (
            extended_statistics
            if not isinstance(extended_statistics, str)
            else json.loads(extended_statistics)
        )

    def get_value(self, name):
        stat_name = name.lower()
        if stat_name not in self._SINGLE_VALUE_STATISTICS:
            return None
        try:
            return getattr(self, stat_name)
        except KeyError as err:
            raise AttributeError(f"'{name}' statistic has not been computed") from err

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    @classmethod
    def from_deequ_json(cls, json_dict: dict) -> "FeatureDescriptiveStatistics":
        stats_dict = {"feature_name": json_dict["column"]}

        if "dataType" in json_dict:
            stats_dict["feature_type"] = json_dict["dataType"]

        if "count" in json_dict and json_dict["count"] == 0:
            # if empty data, ignore the rest of statistics
            stats_dict["count"] = 0
            return cls(**stats_dict)

        # common for all data types
        if "numRecordsNull" in json_dict:
            stats_dict["num_null_values"] = json_dict["numRecordsNull"]
        if "numRecordsNonNull" in json_dict:
            stats_dict["num_non_null_values"] = json_dict["numRecordsNonNull"]
        if "numRecordsNull" in json_dict and "numRecordsNonNull" in json_dict:
            stats_dict["count"] = (
                json_dict["numRecordsNull"] + json_dict["numRecordsNonNull"]
            )
        if "count" in json_dict:
            stats_dict["count"] = json_dict["count"]
        if "completeness" in json_dict:
            stats_dict["completeness"] = json_dict["completeness"]
        if "approximateNumDistinctValues" in json_dict:
            stats_dict["approx_num_distinct_values"] = json_dict[
                "approximateNumDistinctValues"
            ]

        # commmon for all data types if exact_uniqueness is enabled
        if "uniqueness" in json_dict:
            stats_dict["uniqueness"] = json_dict["uniqueness"]
        if "entropy" in json_dict:
            stats_dict["entropy"] = json_dict["entropy"]
        if "distinctness" in json_dict:
            stats_dict["distinctness"] = json_dict["distinctness"]
        if "exactNumDistinctValues" in json_dict:
            stats_dict["exact_num_distinct_values"] = json_dict[
                "exactNumDistinctValues"
            ]

        # fractional / integral features
        if "minimum" in json_dict:
            stats_dict["min"] = json_dict["minimum"]
        if "maximum" in json_dict:
            stats_dict["max"] = json_dict["maximum"]
        if "sum" in json_dict:
            stats_dict["sum"] = json_dict["sum"]
        if "mean" in json_dict:
            stats_dict["mean"] = json_dict["mean"]
        if "stdDev" in json_dict:
            stats_dict["stddev"] = json_dict["stdDev"]
        if "approxPercentiles" in json_dict:
            stats_dict["percentiles"] = json_dict["approxPercentiles"]

        extended_statistics = {}
        if "unique_values" in json_dict:
            extended_statistics["unique_values"] = json_dict["unique_values"]
        if "correlations" in json_dict:
            extended_statistics["correlations"] = json_dict["correlations"]
        if "histogram" in json_dict:
            extended_statistics["histogram"] = json_dict["histogram"]
        if "kll" in json_dict:
            extended_statistics["kll"] = json_dict["kll"]
        stats_dict["extended_statistics"] = (
            extended_statistics if extended_statistics else None
        )

        return cls(**stats_dict)

    def to_dict(self):
        _dict = {
            "id": self._id,
            "featureName": self._feature_name,
            "count": self._count,
            "featureType": self._feature_type,
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
        }
        if self._extended_statistics is not None:
            _dict["extendedStatistics"] = json.dumps(self._extended_statistics)
        return _dict

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
    def feature_type(self) -> str:
        """Data type of the feature. It can be one of Boolean, Fractional, Integral, or String."""
        return self._feature_type

    @property
    def feature_name(self) -> str:
        """Name of the feature."""
        return self._feature_name

    @property
    def count(self) -> int:
        """Number of values."""
        return self._count

    @property
    def completeness(self) -> Optional[float]:
        """Fraction of non-null values in a column."""
        return self._completeness

    @property
    def num_non_null_values(self) -> Optional[int]:
        """Number of non-null values."""
        return self._num_non_null_values

    @property
    def num_null_values(self) -> Optional[int]:
        """Number of null values."""
        return self._num_null_values

    @property
    def approx_num_distinct_values(self) -> Optional[int]:
        """Approximate number of distinct values."""
        return self._approx_num_distinct_values

    @property
    def min(self) -> Optional[float]:
        """Minimum value."""
        return self._min

    @property
    def max(self) -> Optional[float]:
        """Maximum value."""
        return self._max

    @property
    def sum(self) -> Optional[float]:
        """Sum of all feature values."""
        return self._sum

    @property
    def mean(self) -> Optional[float]:
        """Mean value."""
        return self._mean

    @property
    def stddev(self) -> Optional[float]:
        """Standard deviation of the feature values."""
        return self._stddev

    @property
    def percentiles(self) -> Optional[Mapping[str, float]]:
        """Percentiles."""
        return self._percentiles

    @property
    def distinctness(self) -> Optional[float]:
        """Fraction of distinct values of a feature over the number of all its values. Distinct values occur at least once.

        !!! note "Example"
            $[a, a, b]$ contains two distinct values $a$ and $b$, so distinctness is $2/3$.
        """
        return self._distinctness

    @property
    def entropy(self) -> Optional[float]:
        """Entropy is a measure of the level of information contained in an event (feature value) when considering all possible events (all feature values).
        Entropy is estimated using observed value counts as the negative sum of (value_count/total_count) * log(value_count/total_count).

        !!! note "Example"
            $[a, b, b, c, c]$ has three distinct values with counts $[1, 2, 2]$.

            Entropy is then $(-1/5*log(1/5)-2/5*log(2/5)-2/5*log(2/5)) = 1.055$.
        """
        return self._entropy

    @property
    def uniqueness(self) -> Optional[float]:
        """Fraction of unique values over the number of all values of a column. Unique values occur exactly once.

        !!! note "Example"
            $[a, a, b]$ contains one unique value $b$, so uniqueness is $1/3$.
        """
        return self._uniqueness

    @property
    def exact_num_distinct_values(self) -> Optional[int]:
        """Exact number of distinct values."""
        return self._exact_num_distinct_values

    @property
    def extended_statistics(self) -> Optional[dict]:
        """Additional statistics computed on the feature values such as histograms and correlations."""
        return self._extended_statistics
