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

import humps
import json
import warnings

from hsfs import util, engine, training_dataset_feature
from hsfs.statistics_config import StatisticsConfig
from hsfs.storage_connector import StorageConnector
from hsfs.core import (
    query,
    training_dataset_api,
    storage_connector_api,
    training_dataset_engine,
    feed_model_engine,
    statistics_engine,
)


class TrainingDataset:
    HOPSFS = "HOPSFS_TRAINING_DATASET"
    EXTERNAL = "EXTERNAL_TRAINING_DATASET"
    ENTITY_TYPE = "trainingdatasets"

    def __init__(
        self,
        name,
        version,
        data_format,
        location,
        featurestore_id,
        description=None,
        storage_connector=None,
        splits=None,
        seed=None,
        created=None,
        creator=None,
        features=None,
        statistics_config=None,
        featurestore_name=None,
        id=None,
        jobs=None,
        inode_id=None,
        storage_connector_name=None,
        storage_connector_id=None,
        storage_connector_type=None,
        training_dataset_type=None,
        from_query=None,
        querydto=None,
    ):
        self._id = id
        self._name = name
        self._version = version
        self._description = description
        self._data_format = data_format
        self._seed = seed
        self._location = location
        self._from_query = from_query
        self._querydto = querydto

        self._training_dataset_api = training_dataset_api.TrainingDatasetApi(
            featurestore_id
        )

        self._training_dataset_engine = training_dataset_engine.TrainingDatasetEngine(
            featurestore_id
        )

        self._storage_connector_api = storage_connector_api.StorageConnectorApi(
            featurestore_id
        )

        self._statistics_engine = statistics_engine.StatisticsEngine(
            featurestore_id, self.ENTITY_TYPE
        )

        # set up depending on user initialized or coming from backend response
        if training_dataset_type is None:
            # no type -> user init
            self._features = features
            self.storage_connector = storage_connector
            self.splits = splits
            self.statistics_config = statistics_config
        else:
            # type available -> init from backend response
            # make rest call to get all connector information, description etc.
            self._storage_connector = self._storage_connector_api.get_by_id(
                storage_connector_id, storage_connector_type
            )
            self._features = [
                training_dataset_feature.TrainingDatasetFeature.from_response_json(feat)
                for feat in features
            ]
            self._splits = splits
            self._training_dataset_type = training_dataset_type
            self.statistics_config = None

    def save(self, features, write_options={}):
        # TODO: Decide if we want to have potentially dangerous defaults like {}
        if isinstance(features, query.Query):
            feature_dataframe = features.read("offline")
            self._querydto = features
        else:
            feature_dataframe = engine.get_instance().convert_to_default_dataframe(
                features
            )
            self._features = engine.get_instance().parse_schema_training_dataset(
                feature_dataframe
            )

        user_version = self._version
        user_stats_config = self._statistics_config
        self._training_dataset_engine.save(self, feature_dataframe, write_options)
        # currently we do not save the training dataset statistics config for training datasets
        self.statistics_config = user_stats_config
        if self.statistics_config.enabled:
            self._statistics_engine.compute_statistics(self, feature_dataframe)
        if user_version is None:
            warnings.warn(
                "No version provided for creating training dataset `{}`, incremented version to `{}`.".format(
                    self._name, self._version
                ),
                util.VersionWarning,
            )
        return self

    def insert(self, features, overwrite, write_options={}):
        if isinstance(features, query.Query):
            feature_dataframe = features.read()
        else:
            feature_dataframe = engine.get_instance().convert_to_default_dataframe(
                features
            )
        self._training_dataset_engine.insert(
            self, feature_dataframe, write_options, overwrite
        )

        self.compute_statistics()

    def read(self, split=None, read_options={}):
        return self._training_dataset_engine.read(self, split, read_options)

    def compute_statistics(self):
        """Recompute the statistics for the training dataset and save them to the
        feature store.
        """
        if self.statistics_config.enabled:
            return self._statistics_engine.compute_statistics(self, self.read())

    def feed(
        self,
        target_name,
        split=None,
        feature_names=None,
        is_training=True,
        cycle_length=2,
    ):
        """
        :param target_name: name of the target variable
        :type target_name: str, required
        :param split: training dataset split name, train, test or eval, defaults to None
        :type split: str, optional
        :param feature_names: name of training variables, defaults to None
        :type feature_names: 1d array, optional
        :param is_training:  whether it is for training, testing or eval, defaults to True
        :type  is_training: boolean, optional
        :param cycle_length: number of files to be read and deserialized in parallel, defoults to 2
        :type cycle_length: int, optional
        :param engine: execution engine. defaults  to spark
        :type engine: str, optional
        :return: feed model engine object
        :rtype: FeedModelEngine
        """
        return feed_model_engine.FeedModelEngine(
            self,
            split=split,
            target_name=target_name,
            feature_names=feature_names,
            is_training=is_training,
            cycle_length=cycle_length,
        )

    def show(self, n, split=None):
        self.read(split).show(n)

    def add_tag(self, name, value=None):
        """Attach a name/value tag to a training dataset.

        A tag can consist of a name only or a name/value pair. Tag names are
        unique identifiers.

        :param name: name of the tag to be added
        :type name: str
        :param value: value of the tag to be added, defaults to None
        :type value: str, optional
        """
        self._training_dataset_engine.add_tag(self, name, value)

    def delete_tag(self, name):
        """Delete a tag from a training dataset.

        Tag names are unique identifiers.

        :param name: name of the tag to be removed
        :type name: str
        """
        self._training_dataset_engine.delete_tag(self, name)

    def get_tag(self, name=None):
        """Get the tags of a training dataset.

        Tag names are unique identifiers. Returns all tags if no tag name is
        specified.

        :param name: name of the tag to get, defaults to None
        :type name: str, optional
        :return: list of tags as name/value pairs
        :rtype: list of dict
        """
        return self._training_dataset_engine.get_tags(self, name)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("type")
        return cls(**json_decamelized)

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("type")
        # here we lose the information that the user set, e.g. write_options
        self.__init__(**json_decamelized)
        return self

    def _infer_training_dataset_type(self, connector_type):
        if connector_type == StorageConnector.HOPSFS:
            return self.HOPSFS
        elif connector_type == StorageConnector.S3:
            return self.EXTERNAL
        elif connector_type is None:
            return self.HOPSFS
        else:
            raise TypeError(
                "Storage connectors of type {} are currently not supported for training datasets.".format(
                    connector_type
                )
            )

    def json(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def to_dict(self):
        return {
            "name": self._name,
            "version": self._version,
            "description": self._description,
            "dataFormat": self._data_format,
            "storageConnectorId": self._storage_connector.id,
            "location": self._location,
            "trainingDatasetType": self._training_dataset_type,
            "features": self._features,
            "splits": self._splits,
            "seed": self._seed,
            "queryDTO": self._querydto.to_dict() if self._querydto else None,
        }

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, id):
        self._id = id

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, version):
        self._version = version

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, description):
        self._description = description

    @property
    def data_format(self):
        return self._data_format

    @data_format.setter
    def data_format(self, data_format):
        self._data_format = data_format

    @property
    def write_options(self):
        return self._write_options

    @write_options.setter
    def write_options(self, write_options):
        self._write_options = write_options

    @property
    def storage_connector(self):
        return self._storage_connector

    @storage_connector.setter
    def storage_connector(self, storage_connector):
        if isinstance(storage_connector, StorageConnector):
            self._storage_connector = storage_connector
        elif storage_connector is None:
            # init empty connector, otherwise will have to handle it at serialization time
            self._storage_connector = StorageConnector(
                None, None, None, None, None, None, None, None
            )
        else:
            raise TypeError(
                "The argument `storage_connector` has to be `None` or of type `StorageConnector`, is of type: {}".format(
                    type(storage_connector)
                )
            )
        self._training_dataset_type = self._infer_training_dataset_type(
            self._storage_connector.connector_type
        )

    @property
    def splits(self):
        return {split["name"]: split["percentage"] for split in self._splits}

    @splits.setter
    def splits(self, splits):
        # user api differs from how the backend expects the splits to be represented
        splits_list = [{"name": k, "percentage": v} for k, v in splits.items()]
        self._splits = splits_list

    @property
    def location(self):
        return self._location

    @location.setter
    def location(self, location):
        self._location = location

    @property
    def schema(self):
        return self._features

    @property
    def seed(self):
        return self._seed

    @seed.setter
    def seed(self, seed):
        self._seed = seed

    @property
    def statistics_config(self):
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

    @property
    def statistics(self):
        return self._statistics_engine.get_last(self)

    def get_statistics(self, commit_time=None):
        """Returns the statistics for this training dataset at a specific time.

        If `commit_time` is `None`, the most recent statistics are returned.

        :param commit_time: Commit time in the format `YYYYMMDDhhmmss`, defaults to None
        :type commit_time: str, optional
        :return: Statistics information
        :rtype: Statistics
        """
        if commit_time is None:
            return self.statistics
        else:
            return self._statistics_engine.get(self, commit_time)

    @property
    def query(self):
        return self._training_dataset_engine.query(self, "online")

    def get_query(self, storage="online"):
        """Returns the query used to generate this training dataset

        :param storage: the storage for which to return the query, Defaults to "online"  
        :type storage: str, optional
        :return: query 
        :rtype: str 
        """
        return self._training_dataset_engine.query(self, storage)
