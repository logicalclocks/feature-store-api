#
#   Copyright 2020 Logical Clocks AB
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
from typing import Optional, List, Union

from hsfs import util
from hsfs.split_statistics import SplitStatistics
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics


class Statistics:
    DEFAULT_ROW_PERCENTAGE = 1.0

    def __init__(
        self,
        computation_time,
        row_percentage=1.0,
        feature_descriptive_statistics=None,
        # feature group
        feature_group_id=None,
        window_start_commit_time=None,
        window_end_commit_time=None,
        # training dataset
        feature_view_name=None,
        feature_view_version=None,
        training_dataset_version=None,
        split_statistics=None,
        before_transformation=False,
        href=None,
        expand=None,
        items=None,
        count=None,
        type=None,
        **kwargs,
    ):
        self._computation_time = computation_time
        self._feature_descriptive_statistics = self._parse_descriptive_statistics(
            feature_descriptive_statistics
        )
        self._row_percentage = row_percentage
        # feature group
        self._feature_group_id = feature_group_id
        self._window_start_commit_time = window_start_commit_time
        self._window_end_commit_time = window_end_commit_time
        # training dataset
        self._feature_view_name = feature_view_name
        self._feature_view_version = feature_view_version
        self._training_dataset_version = training_dataset_version
        self._split_statistics = self._parse_split_statistics(split_statistics)
        self._before_transformation = before_transformation

    def _parse_descriptive_statistics(
        self,
        desc_statistics: Union[dict, FeatureDescriptiveStatistics],
    ) -> Optional[List[FeatureDescriptiveStatistics]]:
        if desc_statistics is None:
            return None
        elif isinstance(desc_statistics, FeatureDescriptiveStatistics):
            return [desc_statistics]
        elif isinstance(desc_statistics, dict) and "items" not in desc_statistics:
            return [FeatureDescriptiveStatistics.from_response_json(desc_statistics)]
        elif isinstance(desc_statistics, dict) and "items" in desc_statistics:
            return [
                FeatureDescriptiveStatistics.from_response_json(fds)
                for fds in desc_statistics["items"]
            ]
        elif isinstance(desc_statistics, list):
            return [
                (
                    fds
                    if isinstance(fds, FeatureDescriptiveStatistics)
                    else FeatureDescriptiveStatistics.from_response_json(fds)
                )
                for fds in desc_statistics
            ]
        else:
            raise ValueError(
                "Descriptive statistics must be a FeatureDescriptiveStatistics object or a dictionary"
            )

    def _parse_split_statistics(
        self,
        split_statistics,
    ):
        if split_statistics is None:
            return None
        return [
            (
                SplitStatistics.from_response_json(split)
                if isinstance(split, dict)
                else split
            )
            for split in split_statistics
        ]

    @classmethod
    def from_response_json(
        cls, json_dict
    ) -> Optional[Union["Statistics", List["Statistics"]]]:
        json_decamelized: dict = humps.decamelize(json_dict)
        # for consistency, if the json dict contains "count" and "items", we return a list
        # even when there is a single statistics in the list
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0 or len(json_decamelized["items"]) == 0:
                return None
            else:
                return [cls(**config) for config in json_decamelized["items"]]
        else:
            return cls(**json_decamelized)

    def to_dict(self):
        # fg_id, fv_name, fv_version and td_version are already defined in the URI
        _dict = {
            "computationTime": self._computation_time,
            "rowPercentage": self._row_percentage,
            "windowStartCommitTime": self._window_start_commit_time,
            "windowEndCommitTime": self._window_end_commit_time,
            "beforeTransformation": self._before_transformation,
        }
        if self._feature_descriptive_statistics is not None:
            _dict["featureDescriptiveStatistics"] = [
                fds.to_dict() for fds in self._feature_descriptive_statistics
            ]
        if self._split_statistics is not None:
            _dict["splitStatistics"] = [sps.to_dict() for sps in self._split_statistics]
        return _dict

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"Statistics({self._computation_time!r})"

    @property
    def computation_time(self):
        return self._computation_time

    @property
    def row_percentage(self) -> Optional[float]:
        return self._row_percentage

    @row_percentage.setter
    def row_percentage(self, row_percentage: Optional[float]):
        if isinstance(row_percentage, int) or isinstance(row_percentage, float):
            row_percentage = float(row_percentage)
            if row_percentage <= 0.0 or row_percentage > 1.0:
                raise ValueError("Row percentage must be a float between 0 and 1.")
            self._row_percentage = row_percentage
        elif row_percentage is None:
            self._row_percentage = self.DEFAULT_ROW_PERCENTAGE
        else:
            raise TypeError("Row percentage must be a float between 0 and 1.")

    @property
    def feature_descriptive_statistics(self):
        return self._feature_descriptive_statistics

    @property
    def feature_group_id(self):
        return self._feature_group_id

    @property
    def feature_view_name(self):
        return self._feature_view_name

    @property
    def feature_view_version(self):
        return self._feature_view_version

    @property
    def window_start_commit_time(self):
        return self._window_start_commit_time

    @property
    def window_end_commit_time(self):
        return self._window_end_commit_time

    @property
    def training_dataset_version(self):
        return self._training_dataset_version

    @property
    def split_statistics(self):
        return self._split_statistics

    @property
    def before_transformation(self):
        return self._before_transformation
