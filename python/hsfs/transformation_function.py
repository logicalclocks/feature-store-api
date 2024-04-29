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
from typing import List, Optional

import humps
from hsfs import util
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core import transformation_function_engine
from hsfs.decorators import typechecked
from hsfs.hopsworks_udf import HopsworksUdf


@typechecked
class TransformationFunction:
    def __init__(
        self,
        featurestore_id: int,
        hopsworks_udf: HopsworksUdf,
        version: Optional[int] = None,
        id: Optional[int] = None,
        # TODO : Check if the below are actually needed
        type=None,
        items=None,
        count=None,
        href=None,
        **kwargs,
    ):
        self._id = id
        self._featurestore_id = featurestore_id
        self._version = version

        self._transformation_function_engine = (
            transformation_function_engine.TransformationFunctionEngine(
                self._featurestore_id
            )
        )
        if not isinstance(hopsworks_udf, HopsworksUdf):
            raise FeatureStoreException(
                "Use hopsworks_udf decorator when creating the feature view."
            )
        self._hopsworks_udf = hopsworks_udf
        self._feature_group_feature_name: Optional[str] = None
        self._feature_group_id: Optional[int] = None

    def save(self):
        """Persist transformation function in backend.

        !!! example
            ```python
            # define function
            def plus_one(value):
                return value + 1

            # create transformation function
            plus_one_meta = fs.create_transformation_function(
                    transformation_function=plus_one,
                    output_type=int,
                    version=1
                )

            # persist transformation function in backend
            plus_one_meta.save()
            ```
        """
        self._transformation_function_engine.save(self)

    def delete(self):
        """Delete transformation function from backend.

        !!! example
            ```python
            # define function
            def plus_one(value):
                return value + 1

            # create transformation function
            plus_one_meta = fs.create_transformation_function(
                    transformation_function=plus_one,
                    output_type=int,
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

    def __call__(self, *args: List[str]):
        self._hopsworks_udf = self._hopsworks_udf(*args)
        return self

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        print(json_decamelized)
        # TODO : Clean this up.
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

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(**json_decamelized)
        return self

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
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
    def id(self, id: int):
        self._id = id

    @property
    def version(self) -> int:
        return self._version

    @property
    def hopsworks_udf(self) -> HopsworksUdf:
        return self._hopsworks_udf

    @version.setter
    def version(self, version: int):
        self._version = version
