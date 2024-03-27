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
import json
from typing import Any, Dict, List, Optional, Union

import humps
from hsfs import util
from hsfs.constructor import prepared_statement_parameter


class ServingPreparedStatement:
    def __init__(
        self,
        feature_group_id: Optional[int] = None,
        prepared_statement_index: Optional[int] = None,
        prepared_statement_parameters: Optional[
            List["prepared_statement_parameter.PreparedStatementParameter"]
        ] = None,
        query_online: Optional[str] = None,
        prefix: Optional[str] = None,
        type: Optional[str] = None,
        items: Optional[List[Dict[str, Any]]] = None,
        count: Optional[int] = None,
        href: Optional[str] = None,
        **kwargs,
    ):
        self._feature_group_id = feature_group_id
        self._prepared_statement_index = prepared_statement_index
        # use setter to ensure that the parameters are sorted by index
        self.prepared_statement_parameters = prepared_statement_parameters
        self._query_online = query_online
        self._prefix = prefix

    @classmethod
    def from_response_json(
        cls, json_dict: Dict[str, Any]
    ) -> List["ServingPreparedStatement"]:
        json_decamelized = humps.decamelize(json_dict)
        if json_decamelized["count"] == 0:
            return []
        else:
            return [cls(**pstm_dto) for pstm_dto in json_decamelized["items"]]

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(**json_decamelized)
        return self

    def json(self) -> str:
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "preparedStatementIndex": self._prepared_statement_index,
            "preparedStatementParameters": self._prepared_statement_parameters,
            "queryOnline": self._query_online,
        }

    @property
    def feature_group_id(self) -> Optional[int]:
        return self._feature_group_id

    @property
    def prepared_statement_index(self) -> Optional[int]:
        return self._prepared_statement_index

    @property
    def prepared_statement_parameters(
        self,
    ) -> List["prepared_statement_parameter.PreparedStatementParameter"]:
        return self._prepared_statement_parameters

    @property
    def query_online(self) -> Optional[str]:
        return self._query_online

    @property
    def prefix(self) -> Optional[str]:
        return self._prefix

    @feature_group_id.setter
    def feature_group_id(self, feature_group_id: Optional[int]):
        self._feature_group_id = feature_group_id

    @prepared_statement_index.setter
    def prepared_statement_index(self, prepared_statement_index: Optional[int]):
        self._prepared_statement_index = prepared_statement_index

    @prepared_statement_parameters.setter
    def prepared_statement_parameters(
        self,
        prepared_statement_parameters: Union[
            List["prepared_statement_parameter.PreparedStatementParameter"],
            List[Dict[str, Any]],
        ],
    ):
        if isinstance(prepared_statement_parameters[0], dict):
            self._prepared_statement_parameters = sorted(
                [
                    prepared_statement_parameter.PreparedStatementParameter.from_response_json(
                        pstm_param
                    )
                    for pstm_param in prepared_statement_parameters
                ],
                key=lambda x: x.get("index"),
            )
        else:
            self._prepared_statement_parameters = sorted(
                prepared_statement_parameters, key=lambda x: x.index
            )

    @query_online.setter
    def query_online(self, query_online: Optional[str]):
        self._query_online = query_online

    @prefix.setter
    def prefix(self, prefix: Optional[str]):
        self._prefix = prefix
