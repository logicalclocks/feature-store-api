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

from hsfs import feature_group as feature_group_module


class HudiFeatureGroupAlias:
    def __init__(
        self,
        feature_group,
        alias,
        left_feature_group_end_timestamp=None,
        left_feature_group_start_timestamp=None,
        **kwargs,
    ):
        self._feature_group = feature_group_module.FeatureGroup.from_response_json(
            feature_group
        )
        self._alias = alias
        self._left_feature_group_start_timestamp = left_feature_group_start_timestamp
        self._left_feature_group_end_timestamp = left_feature_group_end_timestamp

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    @property
    def feature_group(self):
        return self._feature_group

    @property
    def alias(self):
        return self._alias

    @property
    def left_feature_group_start_timestamp(self):
        return self._left_feature_group_start_timestamp

    @property
    def left_feature_group_end_timestamp(self):
        return self._left_feature_group_end_timestamp
