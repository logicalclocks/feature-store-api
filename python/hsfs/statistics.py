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


class Statistics:
    def __init__(
        self,
        commit_time,
        content=None,
        feature_group_commit_id=None,
        split_statistics=None,
        for_transformation=False,
        href=None,
        expand=None,
        items=None,
        count=None,
        type=None,
        **kwargs,
    ):
        self._commit_time = commit_time
        self._feature_group_commit_id = feature_group_commit_id
        # if split_statistics is provided then content will be None
        if split_statistics is None:
            self._content = json.loads(content)
        else:
            self._content = content
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
        if json_decamelized["count"] == 0:
            return None
        elif len(json_decamelized["items"]) == 1:  # todo what if more?
            return cls(**json_decamelized["items"][0])

    def to_dict(self):
        return {
            "commitTime": self._commit_time,
            "featureGroupCommitId": self._feature_group_commit_id,
            "content": json.dumps(self._content),
            "splitStatistics": self._split_statistics,
            "forTransformation": self._for_transformation,
        }

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    @property
    def commit_time(self):
        return self._commit_time

    @property
    def feature_group_commit_id(self):
        return self._feature_group_commit_id

    @property
    def content(self):
        return self._content

    @property
    def split_statistics(self):
        return self._split_statistics

    @property
    def for_transformation(self):
        return self._for_transformation
