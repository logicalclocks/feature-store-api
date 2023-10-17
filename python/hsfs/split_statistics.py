#
#  Copyright 2021. Logical Clocks AB
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import json
import humps

from hsfs import util


class SplitStatistics:
    def __init__(
        self,
        name,
        content,
        href=None,
        expand=None,
        items=None,
        count=None,
        type=None,
        **kwargs
    ):
        self._name = name
        if not isinstance(content, dict):
            self._content = json.loads(content)
        else:
            self._content = content

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def to_dict(self):
        return {
            "name": self._name,
            "content": json.dumps(self._content),
        }

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    @property
    def name(self):
        return self._name

    @property
    def content(self):
        return self._content
