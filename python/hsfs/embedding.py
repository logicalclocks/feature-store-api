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

from dataclasses import dataclass
import json
from typing import Optional, List
import humps

from hsfs import (
    util,
)


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


@dataclass
class EmbeddingFeature:
    """
    Represents an embedding feature.

    # Arguments
        name: The name of the embedding feature.
        dimension: The dimensionality of the embedding feature.
        similarity_function_type: The type of similarity function used for the embedding feature.
          Available functions are `L2`, `COSINE`, and `DOT_PRODUCT`.
          (default is `SimilarityFunctionType.L2`).
        feature_group: The feature group object that contains the embedding feature.
        embedding_index: `EmbeddingIndex` The index for managing embedding features.
    """

    def __init__(
        self,
        name: str = None,
        dimension: int = None,
        similarity_function_type: SimilarityFunctionType = SimilarityFunctionType.L2,
        feature_group=None,
        embedding_index=None,
    ):
        self._name = name
        self._dimension = dimension
        self._similarity_function_type = similarity_function_type
        self._feature_group = feature_group
        self._embedding_index = embedding_index

    @classmethod
    def from_json_response(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(
            name=json_decamelized.get("name"),
            dimension=json_decamelized.get("dimension"),
            similarity_function_type=json_decamelized.get("similarity_function_type"),
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
        return {
            "name": self._name,
            "dimension": self._dimension,
            "similarityFunctionType": self._similarity_function_type,
        }

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
            self._features = []
        else:
            self._features = features
        self._feature_group = None
        self._col_prefix = col_prefix

    def add_embedding(
        self,
        name: str,
        dimension: int,
        similarity_function_type: Optional[
            SimilarityFunctionType
        ] = SimilarityFunctionType.L2,
    ):
        """
        Adds a new embedding feature to the index.

        Example:
        ```
        embedding_index = EmbeddingIndex()
        embedding_index.add_embedding(name="user_vector", dimension=256)
        ```

        # Arguments
            name: The name of the embedding feature.
            dimension: The dimensionality of the embedding feature.
            similarity_function_type: The type of similarity function to be used.
        """
        self._features.append(
            EmbeddingFeature(name, dimension, similarity_function_type)
        )

    def get_embeddings(self):
        """
        Returns the list of `hsfs.embedding.EmbeddingFeature` objects associated with the index.

        # Returns
            A list of `hsfs.embedding.EmbeddingFeature` objects
        """
        for feat in self._features:
            feat.feature_group = self._feature_group
            feat.embedding_index = self
        return self._features

    @classmethod
    def from_json_response(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(
            index_name=json_decamelized.get("index_name"),
            features=[
                EmbeddingFeature.from_json_response(f)
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
            "features": self._features,
            "colPrefix": self._col_prefix,
        }

    def __repr__(self):
        return self.json()
