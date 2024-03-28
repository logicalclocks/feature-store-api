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

from typing import Any, Dict, List, Optional, Union

import humps
from hsfs import transformation_function as transformation_fn


class TransformationFunctionAttached:
    def __init__(
        self,
        name: str,
        transformation_function: Union[
            Dict[str, Any], "transformation_fn.TransformationFunction"
        ],
        type: Optional[str] = None,
        items: Optional[Dict[str, Any]] = None,
        count: Optional[int] = None,
        href: Optional[str] = None,
        **kwargs,
    ):
        self._name = name
        self._transformation_function = (
            transformation_fn.TransformationFunction.from_response_json(
                transformation_function
            )
            if isinstance(transformation_function, dict)
            else transformation_function
        )

    @classmethod
    def from_response_json(
        cls, json_dict: Dict[str, Any]
    ) -> Union[
        "TransformationFunctionAttached", List["TransformationFunctionAttached"]
    ]:
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [cls(**tffn_dto) for tffn_dto in json_decamelized["items"]]
        else:
            return cls(**json_decamelized)

    def update_from_response_json(
        self, json_dict: Dict[str, Any]
    ) -> "TransformationFunctionAttached":
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(**json_decamelized)
        return self

    @property
    def name(self) -> str:
        """Set feature name."""
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        self._name = name

    @property
    def transformation_function(self) -> "transformation_fn.TransformationFunction":
        return self._transformation_function

    @transformation_function.setter
    def transformation_function(
        self, transformation_function: "transformation_fn.TransformationFunction"
    ) -> None:
        self._transformation_function = transformation_function
