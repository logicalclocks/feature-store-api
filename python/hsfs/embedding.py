#
#   Copyright 2023 Logical Clocks AB
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
from dataclasses import dataclass
from typing import List, Optional

import humps
from hsfs import (
    client,
    util,
)
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core.vector_db_client import VectorDbClient


class SimilarityFunctionType:
    """
    Enumeration class representing different types of similarity functions.

    # Attributes
        L2 (str): Represents L2 norm similarity function.
        COSINE (str): Represents cosine similarity function.
        DOT_PRODUCT (str): Represents dot product similarity function.
    """

    L2 = "l2_norm"
    COSINE = "cosine"
    DOT_PRODUCT = "dot_product"

    def __init__(self) -> None:
        # Fix for the doc
        raise NotImplementedError("This class should not be instantiated.")


@dataclass
class HsmlModel:
    """
    Data class storing the metadata of a hsml model
    """

    model_registry_id: int
    model_name: str
    model_version: int

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)

        return cls(
            model_registry_id=json_decamelized.get("model_registry_id"),
            model_name=json_decamelized.get("model_name"),
            model_version=json_decamelized.get("model_version"),
        )

    @classmethod
    def from_model(cls, model):
        return cls(
            model_registry_id=model.model_registry_id,
            model_name=model.name,
            model_version=model.version,
        )

    # should get from backend because of authorisation check (unshared project etc)
    def get_model(self):
        try:
            from hsml.model import Model
        except ModuleNotFoundError as err:
            raise FeatureStoreException(
                "Model is attached to embedding feature but hsml library is not installed."
                "Install hsml library before getting the feature group."
            ) from err

        path_params = [
            "project",
            client.get_instance()._project_id,
            "modelregistries",
            self.model_registry_id,
            "models",
            self.model_name + "_" + str(self.model_version),
        ]
        query_params = {"expand": "trainingdatasets"}

        model_json = client.get_instance()._send_request(
            "GET", path_params, query_params
        )
        return Model.from_response_json(model_json)

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "modelRegistryId": self.model_registry_id,
            "modelName": self.model_name,
            "modelVersion": self.model_version,
        }


class EmbeddingFeature:
    """
    Represents an embedding feature.

    # Arguments
        name: The name of the embedding feature.
        dimension: The dimensionality of the embedding feature.
        similarity_function_type: The type of similarity function used for the embedding feature.
          Available functions are `L2`, `COSINE`, and `DOT_PRODUCT`.
          (default is `SimilarityFunctionType.L2`).
        model: `hsml.model.Model` A Model in hsml.
        feature_group: The feature group object that contains the embedding feature.
        embedding_index: `EmbeddingIndex` The index for managing embedding features.
    """

    def __init__(
        self,
        name: str = None,
        dimension: int = None,
        similarity_function_type: SimilarityFunctionType = SimilarityFunctionType.L2,
        model=None,
        feature_group=None,
        embedding_index=None,
    ):
        self._name = name
        self._dimension = dimension
        self._similarity_function_type = similarity_function_type
        self._model = model
        self._feature_group = feature_group
        self._embedding_index = embedding_index

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        hsml_model_json = json_decamelized.get("model")
        hsml_model = (
            HsmlModel.from_response_json(hsml_model_json).get_model()
            if hsml_model_json
            else None
        )

        return cls(
            name=json_decamelized.get("name"),
            dimension=json_decamelized.get("dimension"),
            similarity_function_type=json_decamelized.get("similarity_function_type"),
            model=hsml_model,
        )

    @property
    def name(self):
        """str: The name of the embedding feature."""
        return self._name

    @property
    def dimenstion(self):
        """int: The dimensionality of the embedding feature."""
        return self._dimension

    @property
    def similarity_function_type(self):
        """SimilarityFunctionType: The type of similarity function used for the embedding feature."""
        return self._similarity_function_type

    @property
    def model(self):
        """hsml.model.Model: The Model in hsml."""
        return self._model

    @property
    def feature_group(self):
        """FeatureGroup: The feature group object that contains the embedding feature."""
        return self._feature_group

    @feature_group.setter
    def feature_group(self, feature_group):
        """Set the feature group object that contains the embedding feature.

        Args:
            feature_group (FeatureGroup): The feature group object.
        """
        self._feature_group = feature_group

    @property
    def embedding_index(self):
        """EmbeddingIndex: The index for managing embedding features."""
        return self._embedding_index

    @embedding_index.setter
    def embedding_index(self, embedding_index):
        """Set the index for managing embedding features.

        Args:
            embedding_index (EmbeddingIndex): The embedding index object.
        """
        self._embedding_index = embedding_index

    def json(self):
        """Serialize the EmbeddingFeature object to a JSON string."""
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        """
        Convert the EmbeddingFeature object to a dictionary.

        Returns:
            dict: A dictionary representation of the EmbeddingFeature object.
        """
        d = {
            "name": self._name,
            "dimension": self._dimension,
            "similarityFunctionType": self._similarity_function_type,
        }
        if self._model:
            d["model"] = HsmlModel.from_model(self._model)
        return d

    def __repr__(self):
        return self.json()


class EmbeddingIndex:
    """
    Represents an index for managing embedding features.

    # Arguments
        index_name: The name of the embedding index. The name of the project index is used if not provided.
        features: A list of `EmbeddingFeature` objects for the features that
            contain embeddings that should be indexed for similarity search.
        col_prefix: The prefix to be added to column names when using project index.
            It is managed by Hopsworks and should not be provided.

    !!! Example
        ```
        embedding_index = EmbeddingIndex()
        embedding_index.add_embedding(name="user_vector", dimension=256)
        embeddings = embedding_index.get_embeddings()
        ```
    """

    def __init__(
        self,
        index_name: Optional[str] = None,
        features: Optional[List[EmbeddingFeature]] = None,
        col_prefix: Optional[str] = None,
    ):
        self._index_name = index_name
        if features is None:
            self._features = {}
        else:
            self._features = dict([(feat.name, feat) for feat in features])
        self._feature_group = None
        self._col_prefix = col_prefix
        self._vector_db_client = None

    def add_embedding(
        self,
        name: str,
        dimension: int,
        similarity_function_type: Optional[
            SimilarityFunctionType
        ] = SimilarityFunctionType.L2,
        model=None,
    ):
        """
        Adds a new embedding feature to the index.

        Example:
        ```
        embedding_index = EmbeddingIndex()
        embedding_index.add_embedding(name="user_vector", dimension=256)

        # Attach a hsml model to the embedding feature
        embedding_index = EmbeddingIndex()
        embedding_index.add_embedding(name="user_vector", dimension=256, model=hsml_model)
        ```

        # Arguments
            name: The name of the embedding feature.
            dimension: The dimensionality of the embedding feature.
            similarity_function_type: The type of similarity function to be used.
            model (hsml.model.Model, optional): The hsml model used to generate the embedding.
                Defaults to None.
        """
        self._features[name] = EmbeddingFeature(
            name, dimension, similarity_function_type, model=model
        )

    def get_embedding(self, name):
        """
        Returns the `hsfs.embedding.EmbeddingFeature` object associated with the feature name.

        # Arguments
            name (str): The name of the embedding feature.

        # Returns
            `hsfs.embedding.EmbeddingFeature` object
        """
        feat = self._features.get(name)
        if feat:
            feat.feature_group = self._feature_group
            feat.embedding_index = self
        return feat

    def get_embeddings(self):
        """
        Returns the list of `hsfs.embedding.EmbeddingFeature` objects associated with the index.

        # Returns
            A list of `hsfs.embedding.EmbeddingFeature` objects
        """
        for feat in self._features.values():
            feat.feature_group = self._feature_group
            feat.embedding_index = self
        return self._features.values()

    def count(self, options: map = None):
        """
        Count the number of records in the feature group.

        # Arguments
            options: The options used for the request to the vector database.
                The keys are attribute values of the `hsfs.core.opensearch.OpensearchRequestOption` class.

        # Returns
            int: The number of records in the feature group.

        # Raises:
            ValueError: If the feature group is not initialized.
            FeaturestoreException: If an error occurs during the count operation.
        """
        if self._vector_db_client is None:
            self._vector_db_client = VectorDbClient(self._feature_group.select_all())
        return self._vector_db_client.count(self.feature_group, options=options)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(
            index_name=json_decamelized.get("index_name"),
            features=[
                EmbeddingFeature.from_response_json(f)
                for f in json_decamelized.get("features")
            ],
            col_prefix=json_decamelized.get("col_prefix"),
        )

    @property
    def feature_group(self):
        """FeatureGroup: The feature group object that contains the embedding feature."""
        return self._feature_group

    @feature_group.setter
    def feature_group(self, feature_group):
        """Setter for the feature group object."""
        self._feature_group = feature_group

    @property
    def index_name(self):
        """str: The name of the embedding index."""
        return self._index_name

    @property
    def col_prefix(self):
        """str: The prefix to be added to column names."""
        return self._col_prefix

    def json(self):
        """Serialize the EmbeddingIndex object to a JSON string."""
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        """
        Convert the EmbeddingIndex object to a dictionary.

        Returns:
            dict: A dictionary representation of the EmbeddingIndex object.
        """
        return {
            "indexName": self._index_name,
            "features": [feature.to_dict() for feature in self._features.values()],
            "colPrefix": self._col_prefix,
        }

    def __repr__(self):
        return self.json()
