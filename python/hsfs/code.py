#
#   Copyright 2021 Logical Clocks AB
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

from hsfs import util


class Code:
    def __init__(
        self,
        commit_time,
        application_id,
        content=None,
        feature_group_commit_id=None,
        href=None,
        expand=None,
        items=None,
        count=None,
        type=None,
    ):
        self._commit_time = commit_time
        self._application_id = application_id

    def to_dict(self):
        return {
            "commitTime": self._commit_time,
            "applicationId": self._application_id,
        }

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    @property
    def commit_time(self):
        return self._commit_time

    @property
    def application_id(self):
        return self._application_id

    @property
    def feature_group_commit_id(self):
        return self._feature_group_commit_id

    @property
    def content(self):
        return self._content
