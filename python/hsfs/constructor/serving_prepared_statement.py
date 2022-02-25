#
#  Copyright 2021. Logical Clocks AB
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import humps
import json

from hsfs import util
from hsfs.constructor import prepared_statement_parameter


class ServingPreparedStatement:
    def __init__(
        self,
        feature_group_id=None,
        prepared_statement_index=None,
        prepared_statement_parameters=None,
        query_online=None,
        type=None,
        items=None,
        count=None,
        href=None,
    ):
        self._feature_group_id = feature_group_id
        self._prepared_statement_index = prepared_statement_index
        self._prepared_statement_parameters = [
            prepared_statement_parameter.PreparedStatementParameter.from_response_json(
                psp
            )
            for psp in prepared_statement_parameters
        ]
        self._query_online = query_online

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if json_decamelized["count"] == 0:
            return []
        else:
            return [cls(**pstm_dto) for pstm_dto in json_decamelized["items"]]

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(**json_decamelized)
        return self

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "preparedStatementIndex": self._prepared_statement_index,
            "preparedStatementParameters": self._prepared_statement_parameters,
            "queryOnline": self._query_online,
        }

    @property
    def feature_group_id(self):
        return self._feature_group_id

    @property
    def prepared_statement_index(self):
        return self._prepared_statement_index

    @property
    def prepared_statement_parameters(self):
        return self._prepared_statement_parameters

    @property
    def query_online(self):
        return self._query_online

    @feature_group_id.setter
    def feature_group_id(self, feature_group_id):
        self._feature_group_id = feature_group_id

    @prepared_statement_index.setter
    def prepared_statement_index(self, prepared_statement_index):
        self._prepared_statement_index = prepared_statement_index

    @prepared_statement_parameters.setter
    def prepared_statement_parameters(self, prepared_statement_parameters):
        self._prepared_statement_parameters = prepared_statement_parameters

    @query_online.setter
    def query_online(self, query_online):
        self._query_online = query_online
