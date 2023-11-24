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

import humps

from hsfs import (
    util,
)

class SimilarityFunctionType:
    L2 = "l2_norm"
    COSINE = "cosine"
    DOT_PRODUCT = "dot_product"

@dataclass
class EmbeddingFeature:
    name: str
    dimension: int
    similarity_function_type: SimilarityFunctionType = SimilarityFunctionType.L2
    feature_group = None
    embedding = None

    @classmethod
    def from_json_response(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(
            name=json_decamelized.get("name"),
            dimension=json_decamelized.get("dimension"),
            similarity_function_type=json_decamelized.get("similarity_function_type")
        )

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "name": self.name,
            "dimension": self.dimension,
            "similarityFunctionType": self.similarity_function_type
        }

class Embedding:

    def __init__(self, index_name=None, features=None, col_prefix=None):
        self._index_name = index_name
        if features is None:
            self._features = []
        else:
            self._features = features
        self._feature_group = None
        self._col_prefix = col_prefix

    def add_feature(self, name, dimension, similarity_function_type=SimilarityFunctionType.L2):
        self._features.append(EmbeddingFeature(name, dimension, similarity_function_type))

    def get_features(self):
        for feat in self._features:
            feat.feature_group = self._feature_group
            feat.embedding = self
        return self._features

    @classmethod
    def from_json_response(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(
            index_name=json_decamelized.get("index_name"),
            features=[EmbeddingFeature.from_json_response(f) for f in json_decamelized.get("features")],
            col_prefix=json_decamelized.get("col_prefix")
        )

    @property
    def feature_group(self):
        return self._feature_group

    @feature_group.setter
    def feature_group(self, feature_group):
        self._feature_group = feature_group

    @property
    def index_name(self):
        return self._index_name

    @property
    def col_prefix(self):
        return self._col_prefix

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "indexName": self._index_name,
            "features": self._features,
            "colPrefix": self._col_prefix
        }
