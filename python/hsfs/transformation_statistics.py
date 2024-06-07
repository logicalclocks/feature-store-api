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

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Union

import humps


@dataclass
class FeatureTransformationStatistics:
    """
    Data class that contains all the statistics parameters that can be used for transformations.
    """

    feature_name: str
    count: int = None
    # for any feature type
    completeness: Optional[float] = None
    num_non_null_values: Optional[int] = None
    num_null_values: Optional[int] = None
    approx_num_distinct_values: Optional[int] = None
    # for numerical features
    min: Optional[float] = None
    max: Optional[float] = None
    sum: Optional[float] = None
    mean: Optional[float] = None
    stddev: Optional[float] = None
    percentiles: Optional[Mapping[str, float]] = None
    # with exact uniqueness
    distinctness: Optional[float] = None
    entropy: Optional[float] = None
    uniqueness: Optional[float] = None
    exact_num_distinct_values: Optional[int] = None
    extended_statistics: Optional[Union[dict, str]] = None

    def __init__(
        self,
        feature_name: str,
        count: int = None,
        completeness: Optional[float] = None,
        num_non_null_values: Optional[int] = None,
        num_null_values: Optional[int] = None,
        approx_num_distinct_values: Optional[int] = None,
        min: Optional[float] = None,
        max: Optional[float] = None,
        sum: Optional[float] = None,
        mean: Optional[float] = None,
        stddev: Optional[float] = None,
        percentiles: Optional[Mapping[str, float]] = None,
        distinctness: Optional[float] = None,
        entropy: Optional[float] = None,
        uniqueness: Optional[float] = None,
        exact_num_distinct_values: Optional[int] = None,
        extended_statistics: Optional[Union[dict, str]] = None,
        **kwargs,
    ):
        self.feature_name = feature_name
        self.count = count
        self.completeness = completeness
        self.num_non_null_values = num_non_null_values
        self.num_null_values = num_null_values
        self.approx_num_distinct_values = approx_num_distinct_values
        self.min = min
        self.max = max
        self.sum = sum
        self.mean = mean
        self.stddev = stddev
        self.percentiles = percentiles
        self.distinctness = distinctness
        self.entropy = entropy
        self.uniqueness = uniqueness
        self.exact_num_distinct_values = exact_num_distinct_values
        self.extended_statistics = extended_statistics

    @classmethod
    def from_response_json(
        cls: FeatureTransformationStatistics, json_dict: Dict[str, Any]
    ):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)


class TransformationStatistics:
    """
    Class that stores statistics of all features required for a transformation function.
    """

    def __init__(self, *features):
        self._features = features
        self.__dict__.update(
            {feature: self.init_statistics(feature) for feature in features}
        )

    def init_statistics(self, feature_name):
        return FeatureTransformationStatistics(feature_name=feature_name)

    def set_statistics(self, feature_name, statistics: Dict[str, Any]):
        self.__dict__[feature_name] = (
            FeatureTransformationStatistics.from_response_json(statistics)
        )

    def __repr__(self) -> str:
        return ",\n ".join([repr(self.__dict__[feature]) for feature in self._features])
