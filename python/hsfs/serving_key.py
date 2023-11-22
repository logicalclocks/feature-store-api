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

from hsfs import util


class ServingKey:
    def __init__(
        self,
        feature_name,
        join_index,
        feature_group=None,
        required=True,
        prefix="",
        join_on=None,
        ignore_prefix=False,
        **kwargs,
    ):
        self._feature_name = feature_name
        self._feature_group = feature_group
        self._required = required
        self._prefix = prefix or ""  # incoming prefix can be `None`
        self._join_on = join_on
        self._join_index = join_index
        self._ignore_prefix = ignore_prefix

    @classmethod
    def from_response_json(cls, json_dict):
        if json_dict is None:
            return None
        json_decamelized = humps.decamelize(json_dict)
        serving_key = cls(
            feature_name=json_decamelized.get("feature_name", None),
            join_index=json_decamelized.get("join_index", None),
            feature_group=json_decamelized.get("feature_group", None),
            required=json_decamelized.get("required", True),
            prefix=json_decamelized.get("prefix", ""),
            join_on=json_decamelized.get("join_on", None),
        )
        return serving_key

    def to_dict(self):
        return {
            "feature_name": self._feature_name,
            "join_index": self._join_index,
            "feature_group_id": (
                self._feature_group["id"] if self._feature_group is not None else None
            ),
            "required": self._required,
            "prefix": self._prefix,
            "join_on": self._join_on,
        }

    def __str__(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    @property
    def required_serving_key(self):
        if self._required:
            if self._ignore_prefix:
                return self._feature_name
            else:
                return self._prefix + self._feature_name
        else:
            return self._join_on

    @property
    def feature_name(self):
        return self._feature_name

    @property
    def join_index(self):
        return self._join_index

    @property
    def feature_group(self):
        return self._feature_group

    @property
    def required(self):
        return self._required

    @property
    def prefix(self):
        return self._prefix

    @property
    def join_on(self):
        return self._join_on
