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
        content,
        href=None,
        expand=None,
        items=None,
        count=None,
        type=None,
    ):
        self._commit_time = commit_time
        self._content = json.loads(content)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        # for now there is no case where we get multliple commits in one rest
        # call with the client, only in the front-end relevant
        return cls(**json_decamelized)

    def to_dict(self):
        return {"commitTime": self._commit_time, "content": json.dumps(self._content)}

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    @property
    def commit_time(self):
        return self._commit_time

    @property
    def content(self):
        return self._content
