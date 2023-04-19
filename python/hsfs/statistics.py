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

from hsfs import util
from hsfs.split_statistics import SplitStatistics
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics


class Statistics:
    def __init__(
        self,
        commit_time,
        row_percentage=100,
        content=None,
        feature_descriptive_statistics=None,
        # feature group
        # feature_group_commit_id=None,
        feature_group_id=None,
        window_start_commit_id=None,
        window_end_commit_id=None,
        # feature view
        feature_view_name=None,
        feature_view_version=None,
        window_start_event_time=None,
        window_end_event_time=None,
        # training dataset
        training_dataset_id=None,
        split_statistics=None,
        for_transformation=False,
        href=None,
        expand=None,
        items=None,
        count=None,
        type=None,
    ):
        self._commit_time = commit_time
        self._row_percentage = row_percentage
        if split_statistics is None and feature_descriptive_statistics is None:
            self._content = json.loads(content)
        else:  # if split_statistics is provided then content will be None
            self._content = content

        if feature_descriptive_statistics is not None:
            print(
                "Statistics __init__: feature descriptive statistics NOT null. Deserializing..."
            )
            print(feature_descriptive_statistics)
            self._feature_descriptive_statistics = []
            for fds in feature_descriptive_statistics:
                self._feature_descriptive_statistics.append(
                    FeatureDescriptiveStatistics.from_response_json(fds)
                )
            print(type(self._feature_descriptive_statistics))
            print(self._feature_descriptive_statistics)

        self._feature_descriptive_statistics = feature_descriptive_statistics
        # feature group
        # self._feature_group_commit_id = feature_group_commit_id
        self._feature_group_id = feature_group_id
        self._window_start_commit_id = window_start_commit_id
        self._window_end_commit_id = window_end_commit_id
        # feature view
        self._feature_view_name = feature_view_name
        self._feature_view_version = feature_view_version
        self._window_start_event_time = window_start_event_time
        self._window_end_event_time = window_end_event_time
        # training dataset
        self._training_dataset_id = training_dataset_id
        self._split_statistics = (
            [
                SplitStatistics.from_response_json(split)
                if isinstance(split, dict)
                else split
                for split in split_statistics
            ]
            if split_statistics is not None
            else []
        )
        self._for_transformation = for_transformation

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        # Currently getting multiple commits at the same time is not allowed
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return None
            if json_decamelized["count"] == 1:
                return cls(**json_decamelized["items"][0])
            else:
                return [cls(**config) for config in json_decamelized["items"]]
        else:
            return cls(**json_decamelized)

    def to_dict(self):
        return {
            "commitTime": self._commit_time,
            "rowPercentage": self._row_percentage,
            "content": json.dumps(self._content),
            "featureDescriptiveStatistics": [
                fds.to_dict() for fds in self._feature_descriptive_statistics
            ],
            "windowStartCommitId": self._window_start_commit_id,
            "windowEndCommitId": self._window_end_commit_id,
            "windowStartEventTime": self._window_start_event_time,
            "windowEndEventTime": self._window_end_event_time,
            "splitStatistics": self._split_statistics,
            "forTransformation": self._for_transformation,
        }

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    @property
    def commit_time(self):
        return self._commit_time

    @property
    def rowPercentage(self):
        return self._row_percentage

    @property
    def content(self):
        return self._content

    @property
    def feature_descriptive_statistics(self):
        return self._feature_descriptive_statistics

    @property
    def feature_group_id(self):
        return self._feature_group_id

    @property
    def window_start_commit_id(self):
        return self._window_start_commit_id

    @property
    def window_end_commit_id(self):
        return self._window_end_commit_id

    @property
    def feature_view_name(self):
        return self._feature_view_name

    @property
    def feature_view_version(self):
        return self._feature_view_version

    @property
    def window_start_event_time(self):
        return self._window_start_event_time

    @property
    def window_end_event_time(self):
        return self._window_end_event_time

    @property
    def training_dataset_id(self):
        return self._training_dataset_id

    @property
    def split_statistics(self):
        return self._split_statistics

    @property
    def for_transformation(self):
        return self._for_transformation
