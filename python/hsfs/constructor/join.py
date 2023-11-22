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

from hsfs import util
from hsfs.constructor import query
import humps


class Join:
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CROSS = "CROSS"
    LEFT_SEMI_JOIN = "LEFT_SEMI_JOIN"
    COMMA = "COMMA"

    def __init__(self, query, on, left_on, right_on, join_type, prefix, **kwargs):
        self._query = query
        self._on = util.parse_features(on)
        self._left_on = util.parse_features(left_on)
        self._right_on = util.parse_features(right_on)
        self._join_type = join_type or self.INNER
        self._prefix = prefix

    def to_dict(self):
        return {
            "query": self._query,
            "on": self._on,
            "leftOn": self._left_on,
            "rightOn": self._right_on,
            "type": self._join_type,
            "prefix": self._prefix,
        }

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)

        return cls(
            query=query.Query.from_response_json(json_decamelized["query"]),
            on=json_decamelized.get("on", None),
            left_on=json_decamelized.get("left_on", None),
            right_on=json_decamelized.get("right_on", None),
            join_type=json_decamelized.get("type", None),
            prefix=json_decamelized.get("prefix", None),
        )

    @property
    def query(self):
        return self._query

    @query.setter
    def query(self, query):
        self._query = query

    @property
    def prefix(self):
        return self._prefix
