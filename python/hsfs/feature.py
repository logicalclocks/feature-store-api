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
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Union

import hsfs
import humps
from hsfs import util
from hsfs.constructor import filter
from hsfs.decorators import typechecked


@typechecked
class Feature:
    """Metadata object representing a feature in a feature group in the Feature Store.

    See Training Dataset Feature for the
    feature representation of training dataset schemas.
    """

    COMPLEX_TYPES = ["MAP", "ARRAY", "STRUCT", "UNIONTYPE"]

    def __init__(
        self,
        name: str,
        type: Optional[str] = None,
        description: Optional[str] = None,
        primary: bool = False,
        partition: bool = False,
        hudi_precombine_key: bool = False,
        online_type: Optional[str] = None,
        default_value: Optional[str] = None,
        feature_group_id: Optional[int] = None,
        feature_group: Optional[
            Union[
                "hsfs.feature_group.FeatureGroup",
                "hsfs.feature_group.ExternalFeatureGroup",
                "hsfs.feature_group.SpineGroup",
            ]
        ] = None,
        **kwargs,
    ) -> None:
        self._name = util.autofix_feature_name(name)
        self._type = type
        self._description = description
        self._primary = primary
        self._partition = partition
        self._hudi_precombine_key = hudi_precombine_key
        self._online_type = online_type
        self._default_value = default_value
        if feature_group is not None:
            self._feature_group_id = feature_group.id
        else:
            self._feature_group_id = feature_group_id

    def to_dict(self) -> Dict[str, Any]:
        """Get structured info about specific Feature in python dictionary format.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            selected_feature = fg.get_feature("min_temp")
            selected_feature.to_dict()
            ```
        """
        return {
            "name": self._name,
            "type": self._type,
            "description": self._description,
            "partition": self._partition,
            "hudiPrecombineKey": self._hudi_precombine_key,
            "primary": self._primary,
            "onlineType": self._online_type,
            "defaultValue": self._default_value,
            "featureGroupId": self._feature_group_id,
        }

    def json(self) -> str:
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    @classmethod
    def from_response_json(cls, json_dict: Dict[str, Any]) -> "Feature":
        if json_dict is None:
            return None

        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def is_complex(self) -> bool:
        """Returns true if the feature has a complex type.

        !!! example
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            selected_feature = fg.get_feature("min_temp")
            selected_feature.is_complex()
            ```
        """
        return any(map(self._type.upper().startswith, self.COMPLEX_TYPES))

    @property
    def name(self) -> str:
        """Name of the feature."""
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        self._name = name

    @property
    def description(self) -> Optional[str]:
        """Description of the feature."""
        return self._description

    @description.setter
    def description(self, description: Optional[str]) -> None:
        self._description = description

    @property
    def type(self) -> Optional[str]:
        """Data type of the feature in the offline feature store.

        !!! danger "Not a Python type"
            This type property is not to be confused with Python types.
            The type property represents the actual data type of the feature in
            the feature store.
        """
        return self._type

    @type.setter
    def type(self, type: Optional[str]) -> None:
        self._type = type

    @property
    def online_type(self) -> Optional[str]:
        """Data type of the feature in the online feature store."""
        return self._online_type

    @online_type.setter
    def online_type(self, online_type: Optional[str]) -> None:
        self._online_type = online_type

    @property
    def primary(self) -> bool:
        """Whether the feature is part of the primary key of the feature group."""
        return self._primary

    @primary.setter
    def primary(self, primary: bool) -> None:
        self._primary = primary

    @property
    def partition(self) -> bool:
        """Whether the feature is part of the partition key of the feature group."""
        return self._partition

    @partition.setter
    def partition(self, partition: bool) -> None:
        self._partition = partition

    @property
    def hudi_precombine_key(self) -> bool:
        """Whether the feature is part of the hudi precombine key of the feature group."""
        return self._hudi_precombine_key

    @hudi_precombine_key.setter
    def hudi_precombine_key(self, hudi_precombine_key: bool) -> None:
        self._hudi_precombine_key = hudi_precombine_key

    @property
    def default_value(self) -> Optional[str]:
        """Default value of the feature as string, if the feature was appended to the
        feature group."""
        return self._default_value

    @default_value.setter
    def default_value(self, default_value: Optional[str]) -> None:
        self._default_value = default_value

    @property
    def feature_group_id(self) -> Optional[int]:
        return self._feature_group_id

    def __lt__(self, other: Any) -> "filter.Filter":
        return filter.Filter(self, filter.Filter.LT, other)

    def __le__(self, other: Any) -> "filter.Filter":
        return filter.Filter(self, filter.Filter.LE, other)

    def __eq__(self, other: Any) -> "filter.Filter":
        return filter.Filter(self, filter.Filter.EQ, other)

    def __ne__(self, other: Any) -> "filter.Filter":
        return filter.Filter(self, filter.Filter.NE, other)

    def __ge__(self, other: Any) -> "filter.Filter":
        return filter.Filter(self, filter.Filter.GE, other)

    def __gt__(self, other: Any) -> "filter.Filter":
        return filter.Filter(self, filter.Filter.GT, other)

    def contains(self, other: Union[str, List[Any]]) -> "filter.Filter":
        """
        !!! warning "Deprecated"
            `contains` method is deprecated. Use `isin` instead.
        """
        return self.isin(other)

    def isin(self, other: Union[str, List[Any]]) -> "filter.Filter":
        return filter.Filter(self, filter.Filter.IN, json.dumps(other))

    def like(self, other: Any) -> "filter.Filter":
        return filter.Filter(self, filter.Filter.LK, other)

    def __str__(self) -> str:
        return self.json()

    def __repr__(self) -> str:
        return f"Feature({self._name!r}, {self._type!r}, {self._description!r}, {self._primary}, {self._partition}, {self._online_type!r}, {self._default_value!r}, {self._feature_group_id!r})"

    def __hash__(self) -> int:
        return hash(f"{self.feature_group_id}_{self.name}")
