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

from typing import Optional, Union, Any, Dict, List, TypeVar
from datetime import datetime
from hsfs import constructor, tag, util
from hsfs.core import feature_view_engine
import humps
from hsfs.transformation_function import TransformationFunction
from hsfs.statistics_config import StatisticsConfig
from hsfs.constructor import query
import json


class FeatureView:

    def __init__(
        self,
        name: str,
        query,
        featurestore_id,
        id=None,
        version: Optional[int] = None,
        description: Optional[str] = "",
        statistics_config: Optional[Union[StatisticsConfig, bool, dict]] = None,
        label: Optional[List[str]] = [],
        transformation_functions: Optional[Dict[str, TransformationFunction]] = {}
    ):
        self._name = name
        self._id = id
        self._query = query
        self._featurestore_id = featurestore_id
        self._version = version
        self._description = description
        self._statistics_config = statistics_config
        self._label = label
        self._transformation_functions = transformation_functions
        self._features = None
        self._feature_view_engine = feature_view_engine.FeatureViewEngine(
            featurestore_id)

    def save(self):
        self._feature_view_engine.save(self)

    def delete(self):
        self._feature_view_engine.delete(self.name, self.version)

    def update_description(self, description):
        return self

    def get_batch_query(
        self, start_time: Optional[datetime], end_time: Optional[datetime]
    ):
        return self._feature_view_engine.get_batch_query(
            self, start_time, end_time)

    def get_online_vector(
        self,
        entry: Dict[str, Any],
        replace: Optional[Dict],
        external: Optional[bool] = False,
    ):
        return list()

    def get_online_vectors(
        self,
        entry: Dict[str, Any],
        replace: Optional[Dict],
        external: Optional[bool] = False,
    ):
        return list(list())

    def preview_online_vector(self):
        return list()

    def preview_online_vectors(self, n: int):
        return list(list())

    def add_tag(self, name: str, value):
        pass

    def get_tag(self, name: str):
        return tag.Tag()

    def get_tags(self):
        return list(list(tag.Tag()))

    def delete_tag(self, name: str):
        pass

    def register_transformation_statistics(self, version: int):
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
        return cls(
            id=json_decamelized.get("id", None),
            name=json_decamelized["name"],
            query=query.Query.from_response_json(json_decamelized["query"]),
            featurestore_id=json_decamelized["featurestore_id"],
            version=json_decamelized.get("version", None),
            description=json_decamelized.get("description", None),
            statistics_config=json_decamelized.get("statistics_config", None),
            label=json_decamelized.get("label", None)
        )

    def update_from_response_json(self, json_dict):
        other = self.from_response_json(json_dict)
        for key in ["name", 'description', "id", "query", "featurestore_id",
                    "version", "statistics_config", "label"]:
            self._update_if_present(self, other, key)
        return self

    @staticmethod
    def _update_if_present(this, new, key):
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
            "statisticsConfig": self._statistics_config,
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
    def statistics_config(self):
        """Statistics configuration object defining the settings for statistics
        computation of the feature view."""
        return self._statistics_config

    @statistics_config.setter
    def statistics_config(self, statistics_config):
        if isinstance(statistics_config, StatisticsConfig):
            self._statistics_config = statistics_config
        elif isinstance(statistics_config, dict):
            self._statistics_config = StatisticsConfig(**statistics_config)
        elif isinstance(statistics_config, bool):
            self._statistics_config = StatisticsConfig(statistics_config)
        elif statistics_config is None:
            self._statistics_config = StatisticsConfig()
        else:
            raise TypeError(
                "The argument `statistics_config` has to be `None` of type `StatisticsConfig, `bool` or `dict`, but is of type: `{}`".format(
                    type(statistics_config)
                )
            )