#
#   Copyright 2022 Logical Clocks AB
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
from datetime import datetime
from typing import Optional, Any, Dict, List

import humps

from hsfs import tag, util, training_dataset_feature
from hsfs.constructor import query
from hsfs.core import (
    feature_view_engine, statistics_engine,
    transformation_function_engine, vector_server
)
from hsfs.transformation_function import TransformationFunction


class FeatureView:
    ENTITY_TYPE = "featureview"

    def __init__(
        self,
        name: str,
        query,
        featurestore_id,
        id=None,
        version: Optional[int] = None,
        description: Optional[str] = "",
        label: Optional[List[str]] = [],
        transformation_functions: Optional[
            Dict[str, TransformationFunction]] = {}
    ):
        self._name = name
        self._id = id
        self._query = query
        self._featurestore_id = featurestore_id
        self._version = version
        self._description = description
        self._label = label
        self._transformation_functions = transformation_functions
        self._features = None
        self._feature_view_engine = feature_view_engine.FeatureViewEngine(
            featurestore_id)
        self._transformation_function_engine = (
            transformation_function_engine.TransformationFunctionEngine(
                featurestore_id)
        )
        self._statistics_engine = statistics_engine.StatisticsEngine(
            featurestore_id, self.ENTITY_TYPE
        )
        self._vector_server = vector_server.VectorServer(featurestore_id)

    def save(self):
        """Save created feature view object to Hopsworks

        # Returns
            `FeatureView`: updated `FeatureView` after save
        """
        return self._feature_view_engine.save(self)

    def delete(self):
        """Delete current feature view and all associated metadata.

        !!! danger "Potentially dangerous operation"
            This operation drops all metadata associated with **this version** of the
            feature view **and** related training dataset **and** materialized data in HopsFS.

        # Raises
            `RestAPIError`.
        """
        self._feature_view_engine.delete(self.name, self.version)

    def update(self):
        # TODO feature view: wait for RestAPI
        return self

    def init_serving(
        self, training_dataset_version: Optional[int] = 1,
        batch: Optional[bool] = None, external: Optional[bool] = False
    ):
        """Initialise and cache parametrized prepared statement to
           retrieve feature vector from online feature store.

        # Arguments
            training_dataset_version: int, optional. Default to be 1. Transformation statistics
                are fetched from training dataset and apply in serving vector.
            batch: boolean, optional. If set to True, prepared statements will be
                initialised for retrieving serving vectors as a batch.
            external: boolean, optional. If set to True, the connection to the
                online feature store is established using the same host as
                for the `host` parameter in the [`hsfs.connection()`](project.md#connection) method.
                If set to False, the online feature store storage connector is used
                which relies on the private IP.
        """
        self._vector_server.init_serving(self, batch, external)
        self._vector_server.training_dataset_version = training_dataset_version

    def get_batch_query(
        self, start_time: Optional[datetime], end_time: Optional[datetime]
    ):
        """Get a query string of batch query.

        # Arguments
            start_time: Optional. Start time of the batch query.
            end_time: Optional. End time of the batch query.

        # Returns
            `str`: batch query
        """
        return self._feature_view_engine.get_batch_query(
            self, start_time, end_time)

    def get_feature_vector(
        self, entry: Dict[str, Any], external: Optional[bool] = False
    ):
        """Returns assembled serving vector from online feature store.

        # Arguments
            entry: dictionary of training dataset feature group primary key names as keys and values provided by
                serving application.
            external: boolean, optional. If set to True, the connection to the
                online feature store is established using the same host as
                for the `host` parameter in the [`hsfs.connection()`](project.md#connection) method.
                If set to False, the online feature store storage connector is used
                which relies on the private IP.
        # Returns
            `list` List of feature values related to provided primary keys, ordered according to positions of this
            features in training dataset query.
        """
        return self._vector_server.get_feature_vector(self, entry, external)

    def get_feature_vectors(
        self, entry: Dict[str, List[Any]], external: Optional[bool] = False
    ):
        """Returns assembled serving vectors in batches from online feature store.

        # Arguments
            entry: dict of feature group primary key names as keys and value as list of primary keys provided by
                serving application.
            external: boolean, optional. If set to True, the connection to the
                online feature store is established using the same host as
                for the `host` parameter in the [`hsfs.connection()`](project.md#connection) method.
                If set to False, the online feature store storage connector is used
                which relies on the private IP.
        # Returns
            `List[list]` List of lists of feature values related to provided primary keys, ordered according to
            positions of this features in training dataset query.
        """
        return self._vector_server.get_feature_vectors(self, entry, external)

    def preview_feature_vector(self, external: Optional[bool] = False):
        """Returns a sample of assembled serving vector from online feature store.

        # Arguments
            external: boolean, optional. If set to True, the connection to the
                online feature store is established using the same host as
                for the `host` parameter in the [`hsfs.connection()`](project.md#connection) method.
                If set to False, the online feature store storage connector is used
                which relies on the private IP.
        # Returns
            `list` List of feature values, ordered according to positions of this
            features in training dataset query.
        """
        return self._vector_server.get_preview_vectors(self, external, 1)

    def preview_feature_vectors(self, n: int, external: Optional[bool] = False):
        """Returns n samples of assembled serving vectors in batches from online feature store.

        # Arguments
            external: boolean, optional. If set to True, the connection to the
                online feature store is established using the same host as
                for the `host` parameter in the [`hsfs.connection()`](project.md#connection) method.
                If set to False, the online feature store storage connector is used
                which relies on the private IP.
        # Returns
            `List[list]` List of lists of feature values , ordered according to
            positions of this features in training dataset query.
        """
        return self._vector_server.get_preview_vectors(self, external, n)

    def get_batch_data(self, start_time, end_time):
        # return df
        pass

    def create_batch_data(self, start_time, end_time, storage_connector):
        # return None, save to storage connector
        pass

    def add_tag(self, name: str, value):
        pass

    def get_tag(self, name: str):
        return tag.Tag()

    def get_tags(self):
        return list(list(tag.Tag()))

    def delete_tag(self, name: str):
        pass

    def add_training_dataset_tag(self, version: int, name: str, value):
        pass

    def get_training_dataset_tag(self, version: int, name: str):
        return tag.Tag()

    def get_training_dataset_tags(self, version: int):
        return list(list(tag.Tag()))

    def delete_training_dataset_tag(self, version: int, name: str):
        pass

    def get_training_data(
        self,
        version: int,
        split: str,
    ):
        pass

    def purge_training_data(self, version: int):
        pass

    def purge_all_training_data(self):
        pass

    def delete_training_dataset(self, version: int):
        pass

    def delete_all_training_datasets(self):
        pass

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        fv = cls(
            id=json_decamelized.get("id", None),
            name=json_decamelized["name"],
            query=query.Query.from_response_json(json_decamelized["query"]),
            featurestore_id=json_decamelized["featurestore_id"],
            version=json_decamelized.get("version", None),
            description=json_decamelized.get("description", None),
            label=json_decamelized.get("label", None)
        )
        features = json_decamelized.get("features", None)
        if features:
            features = [
                training_dataset_feature.TrainingDatasetFeature.from_response_json(
                    feature)
                for feature in features]
        fv.schema = features
        return fv

    def update_from_response_json(self, json_dict):
        other = self.from_response_json(json_dict)
        for key in ["name", 'description', "id", "query", "featurestore_id",
                    "version", "statistics_config", "label"]:
            self._update_attribute_if_present(self, other, key)
        return self

    @staticmethod
    def _update_attribute_if_present(this, new, key):
        if getattr(new, key):
            setattr(this, key, getattr(new, key))

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "name": self._name,
            "version": self._version,
            "description": self._description,
            "query": self._query,
            "features": self._features,
            "label": self._label
        }

    @property
    def id(self):
        """Feature view id."""
        return self._id

    @id.setter
    def id(self, id):
        self._id = id

    @property
    def featurestore_id(self):
        """Feature store id."""
        return self._featurestore_id

    @featurestore_id.setter
    def featurestore_id(self, id):
        self._featurestore_id = id

    @property
    def name(self):
        """Name of the feature view."""
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def version(self):
        """Version number of the feature view."""
        return self._version

    @version.setter
    def version(self, version):
        self._version = version

    @property
    def label(self):
        """The label/prediction feature of the feature view.

        Can be a composite of multiple features.
        """
        return self._label

    @label.setter
    def label(self, label):
        self._label = [lb.lower() for lb in label]

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, description):
        """Description of the feature view."""
        self._description = description

    @property
    def query(self):
        return self._query

    @query.setter
    def query(self, query_obj):
        """Query of the feature view."""
        self._query = query_obj

    @property
    def transformation_functions(self):
        """Set transformation functions."""
        if self._id is not None and self._transformation_functions is None:
            self._transformation_functions = (
                self._transformation_function_engine.get_td_transformation_fn(
                    self)
            )
        return self._transformation_functions

    @transformation_functions.setter
    def transformation_functions(self, transformation_functions):
        self._transformation_functions = transformation_functions

    @property
    def schema(self):
        """Feature view schema."""
        return self._features

    @schema.setter
    def schema(self, features):
        self._features = features
