#
#   Copyright 2023 Hopsworks AB
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
from typing import Optional
from hsfs import util


class FeatureMonitoringConfig:
    def __init__(
        self,
        feature_group_id: int,
        feature_store_id: int,
        feature_name: str,
        id: Optional[int] = None,
        href: Optional[str] = None,
    ):

        self._id = id
        self._href = href
        self._feature_group_id = feature_group_id
        self._feature_store_id = feature_store_id
        self._feature_name = feature_name

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [
                cls(**expectation_suite)
                for expectation_suite in json_decamelized["items"]
            ]
        else:
            return cls(**json_decamelized)

    def to_dict(self):

        return {
            "feature_store_id": self._feature_store_id,
            "feature_group_id": self._feature_group_id,
            "feature_name": self._feature_name,
        }

    def json(self) -> str:
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def __str__(self):
        return self.json()
