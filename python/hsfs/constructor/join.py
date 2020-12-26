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


class Join:
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CROSS = "CROSS"
    LEFT_SEMI_JOIN = "LEFT_SEMI_JOIN"
    COMMA = "COMMA"

    def __init__(self, query, on, left_on, right_on, join_type):

        self._query = query
        self._on = util.parse_features(on)
        self._left_on = util.parse_features(left_on)
        self._right_on = util.parse_features(right_on)
        self._join_type = join_type or self.INNER

    def to_dict(self):
        return {
            "query": self._query,
            "on": self._on,
            "leftOn": self._left_on,
            "rightOn": self._right_on,
            "type": self._join_type,
        }

    @property
    def query(self):
        return self._query

    @query.setter
    def query(self, query):
        self._query = query
