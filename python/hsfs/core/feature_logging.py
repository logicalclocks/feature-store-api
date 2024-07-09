import json
from typing import Any, Dict

import humps
from hsfs import feature_group, util


class FeatureLogging:

    def __init__(self, id: int,
                 transformed_features: "feature_group.FeatureGroup",
                 untransformed_features: "feature_group.FeatureGroup"):
        self._id = id
        self._transformed_features = transformed_features
        self._untransformed_features = untransformed_features

    @classmethod
    def from_response_json(cls, json_dict: Dict[str, Any]) -> 'FeatureLogging':
        from hsfs.feature_group import FeatureGroup  # avoid circular import
        json_decamelized = humps.decamelize(json_dict)
        transformed_features = json_decamelized.get('transformed_log')
        untransformed_features = json_decamelized.get('untransformed_log')
        if transformed_features:
            transformed_features = FeatureGroup.from_response_json(transformed_features)
        if untransformed_features:
            untransformed_features = FeatureGroup.from_response_json(untransformed_features)
        return cls(json_decamelized.get('id'), transformed_features, untransformed_features)

    @property
    def transformed_features(self) -> "feature_group.FeatureGroup":
        return self._transformed_features

    @property
    def untransformed_features(self) -> "feature_group.FeatureGroup":
        return self._untransformed_features

    @property
    def id(self) -> str:
        return self._id

    def to_dict(self):
        return {
            'id': self._id,
            'transformed_log': self._transformed_features,
            'untransformed_log': self._untransformed_features,
        }

    def json(self) -> Dict[str, Any]:
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def __repr__(self):
        return self.json()

