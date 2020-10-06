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

import humps
import json

from hsfs import util


class FeatureGroupCommit:
    def __init__(
        self,
        commitid=None,
        commitdatestring=None,
        rowsinserted=None,
        rowsupdated=None,
        rowsdeleted=None,
    ):

        self._commitid = commitid
        self._commitdatestring = commitdatestring
        self._rowsinserted = rowsinserted
        self._rowsupdated = rowsupdated
        self._rowsdeleted = rowsdeleted

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(**json_decamelized)
        return self

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "commitID": self._commitid,
            "commitDateString": self._commitdatestring,
            "rowsInserted": self._rowsinserted,
            "rowsUpdated": self._rowsupdated,
            "rowsDeleted": self._rowsdeleted,
        }

    @property
    def commitid(self):
        return self._commitid

    @property
    def commitdatestring(self):
        return self._commitdatestring

    @property
    def rowsinserted(self):
        return self._rowsinserted

    @property
    def rowsupdated(self):
        return self._rowsupdated

    @property
    def rowsdeleted(self):
        return self._rowsdeleted

    @commitid.setter
    def commitid(self, commitid):
        self._commitid = commitid

    @commitdatestring.setter
    def commitdatestring(self, commitdatestring):
        self._commitdatestring = commitdatestring

    @rowsinserted.setter
    def rowsinserted(self, rowsinserted):
        self._rowsinserted = rowsinserted

    @rowsupdated.setter
    def rowsupdated(self, rowsupdated):
        self._rowsupdated = rowsupdated

    @rowsdeleted.setter
    def rowsdeleted(self, rowsdeleted):
        self._rowsdeleted = rowsdeleted
