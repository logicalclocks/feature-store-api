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
        commit_date_string=None,
        rows_inserted=None,
        rows_updated=None,
        rows_deleted=None,
        validation_id=None,
        commit_time=None,
        type=None,
        items=None,
        count=None,
        href=None,
    ):
        self._commitid = commitid
        self._commit_date_string = commit_date_string
        self._commit_time = commit_time
        self._rows_inserted = rows_inserted
        self._rows_updated = rows_updated
        self._rows_deleted = rows_deleted
        self._validation_id = validation_id

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if json_decamelized["count"] >= 1:
            return [cls(**commit_dto) for commit_dto in json_decamelized["items"]]
        return cls(**json_decamelized)

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("type")
        _ = json_decamelized.pop("href")
        self.__init__(**json_decamelized)
        return self

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "commitID": self._commitid,
            "commitDateString": self._commit_date_string,
            "commitTime": self._commit_time,
            "rowsInserted": self._rows_inserted,
            "rowsUpdated": self._rows_updated,
            "rowsDeleted": self._rows_deleted,
            "validationId": self._validation_id,
        }

    @property
    def commitid(self):
        return self._commitid

    @property
    def commit_date_string(self):
        return self._commit_date_string

    @property
    def commit_time(self):
        return self._commit_time

    @property
    def rows_inserted(self):
        return self._rows_inserted

    @property
    def rows_updated(self):
        return self._rows_updated

    @property
    def rows_deleted(self):
        return self._rows_deleted

    @property
    def validation_id(self):
        return self._validation_id

    @commitid.setter
    def commitid(self, commitid):
        self._commitid = commitid

    @commit_time.setter
    def commit_time(self, commit_time):
        self._commit_time = commit_time

    @rows_inserted.setter
    def rows_inserted(self, rows_inserted):
        self._rows_inserted = rows_inserted

    @rows_updated.setter
    def rows_updated(self, rows_updated):
        self._rows_updated = rows_updated

    @rows_deleted.setter
    def rows_deleted(self, rows_deleted):
        self._rows_deleted = rows_deleted

    @validation_id.setter
    def validation_id(self, validation_id):
        self._validation_id = validation_id
