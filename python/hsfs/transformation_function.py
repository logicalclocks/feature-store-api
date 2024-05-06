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
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Union

import humps
from hsfs import util
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core import transformation_function_engine
from hsfs.decorators import typechecked
from hsfs.hopsworks_udf import HopsworksUdf


@typechecked
class TransformationFunction:
    """
    Main DTO class for transformation functions.

    Attributes
    ----------
        id (int) : Id of transformation function.
        version (int) : Version of transformation function.
        hopsworks_udf (HopsworksUdf): Meta data class for user defined functions.
    """

    def __init__(
        self,
        featurestore_id: int,
        hopsworks_udf: HopsworksUdf,
        version: Optional[int] = None,
        id: Optional[int] = None,
        type=None,
        items=None,
        count=None,
        href=None,
        **kwargs,
    ):
        self._id: int = id
        self._featurestore_id: int = featurestore_id
        self._version: int = version

        self._transformation_function_engine = (
            transformation_function_engine.TransformationFunctionEngine(
                self._featurestore_id
            )
        )
        if not isinstance(hopsworks_udf, HopsworksUdf):
            raise FeatureStoreException(
                "Please use the hopsworks_udf decorator when defining transformation functions."
            )

        self._hopsworks_udf: HopsworksUdf = hopsworks_udf

    def save(self) -> None:
        """Save a transformation function into the backend.

        !!! example
            ```python
            # import hopsworks udf decorator
            from hsfs.hopsworks_udf import HopsworksUdf
            # define function
            @hopsworks_udf(int)
            def plus_one(value):
                return value + 1

            # create transformation function
            plus_one_meta = fs.create_transformation_function(
                    transformation_function=plus_one,
                    version=1
                )

            # persist transformation function in backend
            plus_one_meta.save()
            ```
        """
        self._transformation_function_engine.save(self)

    def delete(self) -> None:
        """Delete transformation function from backend.

        !!! example
            ```python
            # import hopsworks udf decorator
            from hsfs.hopsworks_udf import HopsworksUdf
            # define function
            @hopsworks_udf(int)
            def plus_one(value):
                return value + 1

            # create transformation function
            plus_one_meta = fs.create_transformation_function(
                    transformation_function=plus_one,
                    version=1
                )
            # persist transformation function in backend
            plus_one_meta.save()

            # retrieve transformation function
            plus_one_fn = fs.get_transformation_function(name="plus_one")

            # delete transformation function from backend
            plus_one_fn.delete()
            ```
        """
        self._transformation_function_engine.delete(self)

    def __call__(self, *features: List[str]) -> TransformationFunction:
        """
        Update the feature to be using in the transformation function

        # Arguments
            features: `List[str]`. Name of features to be passed to the User Defined function
        # Returns
            `HopsworksUdf`: Meta data class for the user defined function.
        # Raises
            `FeatureStoreException: If the provided number of features do not match the number of arguments in the defined UDF or if the provided feature names are not strings.
        """
        self._hopsworks_udf = self._hopsworks_udf(*features)
        return self

    @classmethod
    def from_response_json(
        cls, json_dict: Dict[str, Any]
    ) -> Union[TransformationFunction, List[TransformationFunction]]:
        """
        Function that constructs the class object from its json serialization.

        # Arguments
            json_dict: `Dict[str, Any]`. Json serialized dictionary for the class.
        # Returns
            `TransformationFunction`: Json deserialized class object.
        """
        json_decamelized = humps.decamelize(json_dict)

        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            for tffn_dto in json_decamelized["items"]:
                if tffn_dto.get("hopsworks_udf", False):
                    tffn_dto["hopsworks_udf"] = HopsworksUdf.from_response_json(
                        tffn_dto["hopsworks_udf"]
                    )
            return [cls(**tffn_dto) for tffn_dto in json_decamelized["items"]]
        else:
            if json_decamelized.get("hopsworks_udf", False):
                json_decamelized["hopsworks_udf"] = HopsworksUdf.from_response_json(
                    json_decamelized["hopsworks_udf"]
                )
            return cls(**json_decamelized)

    def update_from_response_json(
        self, json_dict: Dict[str, Any]
    ) -> TransformationFunction:
        """
        Function that updates the class object from its json serialization.

        # Arguments
            json_dict: `Dict[str, Any]`. Json serialized dictionary for the class.
        # Returns
            `TransformationFunction`: Json deserialized class object.
        """
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(**json_decamelized)
        return self

    def json(self) -> str:
        """
        Convert class into its json serialized form.

        # Returns
            `str`: Json serialized object.
        """
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert class into a dictionary.

        # Returns
            `Dict`: Dictionary that contains all data required to json serialize the object.
        """
        return {
            "id": self._id,
            "version": self._version,
            "featurestoreId": self._featurestore_id,
            "hopsworksUdf": self._hopsworks_udf,
        }

    @property
    def id(self) -> id:
        """Transformation function id."""
        return self._id

    @id.setter
    def id(self, id: int) -> None:
        self._id = id

    @property
    def version(self) -> int:
        """Version of the transformation function."""
        return self._version

    @version.setter
    def version(self, version: int) -> None:
        self._version = version

    @property
    def hopsworks_udf(self) -> HopsworksUdf:
        """Meta data class for the user defined transformation function."""
        return self._hopsworks_udf
