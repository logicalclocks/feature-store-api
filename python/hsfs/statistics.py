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


class Statistics:
    def __init__(
        self,
        commit_time,
        feature_group_commit_id,
        content,
        href=None,
        expand=None,
        items=None,
        count=None,
        type=None,
    ):
        self._commit_time = commit_time
        self._feature_group_commit_id = feature_group_commit_id
        self._content = json.loads(content)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        # Currently getting multiple commits at the same time is not allowed
        if json_decamelized["count"] == 0:
            return None
        elif len(json_decamelized["items"]) == 1:
            return cls(**json_decamelized["items"][0])

    def to_dict(self):
        return {
            "commitTime": self._commit_time,
            "featureGroupCommitId": self._feature_group_commit_id,
            "content": json.dumps(self._content),
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
