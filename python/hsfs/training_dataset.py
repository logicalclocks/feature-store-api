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

import json
import warnings
from typing import Optional, Union, Any, Dict, List, TypeVar

import humps
import pandas as pd
import numpy as np

from datetime import datetime, date

from hsfs import util, engine, training_dataset_feature, client
from hsfs.training_dataset_split import TrainingDatasetSplit
from hsfs.statistics_config import StatisticsConfig
from hsfs.storage_connector import StorageConnector, HopsFSConnector
from hsfs.core import (
    training_dataset_api,
    training_dataset_engine,
    statistics_engine,
    code_engine,
    transformation_function_engine,
    vector_server,
)
from hsfs.constructor import query, filter


class TrainingDataset:
    HOPSFS = "HOPSFS_TRAINING_DATASET"
    EXTERNAL = "EXTERNAL_TRAINING_DATASET"
    IN_MEMORY = "IN_MEMORY_TRAINING_DATASET"
    ENTITY_TYPE = "trainingdatasets"

    def __init__(
        self,
        name,
        version,
        data_format,
        featurestore_id,
        location="",
        event_start_time=None,
        event_end_time=None,
        coalesce=False,
        description=None,
        storage_connector=None,
        splits=None,
        validation_size=None,
        test_size=None,
        train_start=None,
        train_end=None,
        validation_start=None,
        validation_end=None,
        test_start=None,
        test_end=None,
        seed=None,
        created=None,
        creator=None,
        features=None,
        statistics_config=None,
        featurestore_name=None,
        id=None,
        inode_id=None,
        training_dataset_type=None,
        from_query=None,
        querydto=None,
        label=None,
        transformation_functions=None,
        train_split=None,
        time_split_size=None,
        extra_filter=None,
        **kwargs,
    ):
        self._id = id
        self._name = name
        self._version = version
        self._description = description
        self._data_format = data_format
        self._validation_size = validation_size
        self._test_size = test_size
        self._train_start = train_start
        self._train_end = train_end
        self._validation_start = validation_start
        self._validation_end = validation_end
        self._test_start = test_start
        self._test_end = test_end
        self._coalesce = coalesce
        self._seed = seed
        self._location = location
        self._from_query = from_query
        self._querydto = querydto
        self._feature_store_id = featurestore_id
        self._feature_store_name = featurestore_name
        self._transformation_functions = transformation_functions
        self._train_split = train_split
        self._training_dataset_api = training_dataset_api.TrainingDatasetApi(
            featurestore_id
        )

        self._training_dataset_engine = training_dataset_engine.TrainingDatasetEngine(
            featurestore_id
        )

        self._statistics_engine = statistics_engine.StatisticsEngine(
            featurestore_id, self.ENTITY_TYPE
        )

        self._code_engine = code_engine.CodeEngine(featurestore_id, self.ENTITY_TYPE)

        self._transformation_function_engine = (
            transformation_function_engine.TransformationFunctionEngine(featurestore_id)
        )
        if training_dataset_type:
            self.training_dataset_type = training_dataset_type
        else:
            self._training_dataset_type = None
        # set up depending on user initialized or coming from backend response
        if created is None:
            self._start_time = util.convert_event_time_to_timestamp(event_start_time)
            self._end_time = util.convert_event_time_to_timestamp(event_end_time)
            # no type -> user init
            self._features = features
            self.storage_connector = storage_connector
            self.splits = splits
            self.statistics_config = statistics_config
            self._label = label
            if validation_size or test_size:
                self._train_split = TrainingDatasetSplit.TRAIN
                self.splits = {
                    TrainingDatasetSplit.TRAIN: 1
                    - (validation_size or 0)
                    - (test_size or 0),
                    TrainingDatasetSplit.VALIDATION: validation_size,
                    TrainingDatasetSplit.TEST: test_size,
                }
            self._set_time_splits(
                time_split_size,
                train_start,
                train_end,
                validation_start,
                validation_end,
                test_start,
                test_end,
            )
            self._extra_filter = (
                filter.Logic(filter.Logic.SINGLE, left_f=extra_filter)
                if isinstance(extra_filter, filter.Filter)
                else extra_filter
            )
        else:
            self._start_time = event_start_time
            self._end_time = event_end_time
            # type available -> init from backend response
            # make rest call to get all connector information, description etc.
            self._storage_connector = StorageConnector.from_response_json(
                storage_connector
            )

            if features is None:
                features = []
            self._features = [
                training_dataset_feature.TrainingDatasetFeature.from_response_json(feat)
                for feat in features
            ]
            self._splits = [
                TrainingDatasetSplit.from_response_json(split) for split in splits
            ]
            self._statistics_config = StatisticsConfig.from_response_json(
                statistics_config
            )
            self._label = [feat.name.lower() for feat in self._features if feat.label]
            self._extra_filter = filter.Logic.from_response_json(extra_filter)
        self._vector_server = vector_server.VectorServer(
            featurestore_id, features=self._features
        )

    def _set_time_splits(
        self,
        time_split_size,
        train_start=None,
        train_end=None,
        validation_start=None,
        validation_end=None,
        test_start=None,
        test_end=None,
    ):
        train_start = util.convert_event_time_to_timestamp(train_start)
        train_end = util.convert_event_time_to_timestamp(train_end)
        validation_start = util.convert_event_time_to_timestamp(validation_start)
        validation_end = util.convert_event_time_to_timestamp(validation_end)
        test_start = util.convert_event_time_to_timestamp(test_start)
        test_end = util.convert_event_time_to_timestamp(test_end)

        time_splits = list()
        self._append_time_split(
            time_splits,
            split_name=TrainingDatasetSplit.TRAIN,
            start_time=train_start,
            end_time=train_end or validation_start or test_start,
        )
        if time_split_size == 3:
            self._append_time_split(
                time_splits,
                split_name=TrainingDatasetSplit.VALIDATION,
                start_time=validation_start or train_end,
                end_time=validation_end or test_start,
            )
        self._append_time_split(
            time_splits,
            split_name=TrainingDatasetSplit.TEST,
            start_time=test_start or validation_end or train_end,
            end_time=test_end,
        )
        if time_splits:
            self._train_split = TrainingDatasetSplit.TRAIN
            # prioritise time split
            self._splits = time_splits

    def _append_time_split(
        self,
        time_splits,
        split_name,
        start_time=None,
        end_time=None,
    ):
        if start_time or end_time:
            time_splits.append(
                TrainingDatasetSplit(
                    name=split_name,
                    split_type=TrainingDatasetSplit.TIME_SERIES_SPLIT,
                    start_time=start_time,
                    end_time=end_time,
                )
            )

    def save(
        self,
        features: Union[
            query.Query,
            pd.DataFrame,
            TypeVar("pyspark.sql.DataFrame"),  # noqa: F821
            TypeVar("pyspark.RDD"),  # noqa: F821
            np.ndarray,
            List[list],
        ],
        write_options: Optional[Dict[Any, Any]] = {},
    ):
        """Materialize the training dataset to storage.

        This method materializes the training dataset either from a Feature Store
        `Query`, a Spark or Pandas `DataFrame`, a Spark RDD, two-dimensional Python
        lists or Numpy ndarrays.
        From v2.5 onward, filters are saved along with the `Query`.

        !!! warning "Engine Support"
            Creating Training Datasets from Dataframes is only supported using Spark as Engine.

        # Arguments
            features: Feature data to be materialized.
            write_options: Additional write options as key-value pairs, defaults to `{}`.
                When using the `python` engine, write_options can contain the
                following entries:
                * key `spark` and value an object of type
                [hsfs.core.job_configuration.JobConfiguration](../job_configuration)
                  to configure the Hopsworks Job used to compute the training dataset.
                * key `wait_for_job` and value `True` or `False` to configure
                  whether or not to the save call should return only
                  after the Hopsworks Job has finished. By default it waits.

        # Returns
            `Job`: When using the `python` engine, it returns the Hopsworks Job
                that was launched to create the training dataset.

        # Raises
            `hsfs.client.exceptions.RestAPIError`: Unable to create training dataset metadata.
        """
        user_version = self._version
        user_stats_config = self._statistics_config
        # td_job is used only if the python engine is used
        training_dataset, td_job = self._training_dataset_engine.save(
            self, features, write_options
        )
        self.storage_connector = training_dataset.storage_connector
        # currently we do not save the training dataset statistics config for training datasets
        self.statistics_config = user_stats_config
        self._code_engine.save_code(self)
        if self.statistics_config.enabled and engine.get_type() == "spark":
            self.compute_statistics()
        if user_version is None:
            warnings.warn(
                "No version provided for creating training dataset `{}`, incremented version to `{}`.".format(
                    self._name, self._version
                ),
                util.VersionWarning,
            )

        return td_job

    def insert(
        self,
        features: Union[
            query.Query,
            pd.DataFrame,
            TypeVar("pyspark.sql.DataFrame"),  # noqa: F821
            TypeVar("pyspark.RDD"),  # noqa: F821
            np.ndarray,
            List[list],
        ],
        overwrite: bool,
        write_options: Optional[Dict[Any, Any]] = {},
    ):
        """Insert additional feature data into the training dataset.

        !!! warning "Deprecated"
            `insert` method is deprecated.

        This method appends data to the training dataset either from a Feature Store
        `Query`, a Spark or Pandas `DataFrame`, a Spark RDD, two-dimensional Python
        lists or Numpy ndarrays. The schemas must match for this operation.

        This can also be used to overwrite all data in an existing training dataset.

        # Arguments
            features: Feature data to be materialized.
            overwrite: Whether to overwrite the entire data in the training dataset.
            write_options: Additional write options as key-value pairs, defaults to `{}`.
                When using the `python` engine, write_options can contain the
                following entries:
                * key `spark` and value an object of type
                [hsfs.core.job_configuration.JobConfiguration](../job_configuration)
                  to configure the Hopsworks Job used to compute the training dataset.
                * key `wait_for_job` and value `True` or `False` to configure
                  whether or not to the insert call should return only
                  after the Hopsworks Job has finished. By default it waits.

        # Returns
            `Job`: When using the `python` engine, it returns the Hopsworks Job
                that was launched to create the training dataset.

        # Raises
            `hsfs.client.exceptions.RestAPIError`: Unable to create training dataset metadata.
        """
        # td_job is used only if the python engine is used
        td_job = self._training_dataset_engine.insert(
            self, features, write_options, overwrite
        )

        self._code_engine.save_code(self)
        self.compute_statistics()

        return td_job

    def read(self, split=None, read_options={}):
        """Read the training dataset into a dataframe.

        It is also possible to read only a specific split.

        # Arguments
            split: Name of the split to read, defaults to `None`, reading the entire
                training dataset. If the training dataset has split, the `split` parameter
                is mandatory.
            read_options: Additional read options as key/value pairs, defaults to `{}`.
        # Returns
            `DataFrame`: The spark dataframe containing the feature data of the
                training dataset.
        """
        if self.splits and split is None:
            raise ValueError(
                "The training dataset has splits, please specify the split you want to read"
            )

        return self._training_dataset_engine.read(self, split, read_options)

    def compute_statistics(self):
        """Recompute the statistics for the training dataset and save them to the
        feature store.
        """
        if self.statistics_config.enabled and engine.get_type() == "spark":
            if self.splits:
                return self._statistics_engine.register_split_statistics(self)
            else:
                return self._statistics_engine.compute_statistics(self, self.read())

    def show(self, n: int, split: str = None):
        """Show the first `n` rows of the training dataset.

        You can specify a split from which to retrieve the rows.

        # Arguments
            n: Number of rows to show.
            split: Name of the split to show, defaults to `None`, showing the first rows
                when taking all splits together.
        """
        self.read(split).show(n)

    def add_tag(self, name: str, value):
        """Attach a tag to a training dataset.

        A tag consists of a <name,value> pair. Tag names are unique identifiers across the whole cluster.
        The value of a tag can be any valid json - primitives, arrays or json objects.

        # Arguments
            name: Name of the tag to be added.
            value: Value of the tag to be added.

        # Raises
            `hsfs.client.exceptions.RestAPIError` in case the backend fails to add the tag.
        """
        self._training_dataset_engine.add_tag(self, name, value)

    def delete_tag(self, name: str):
        """Delete a tag attached to a training dataset.

        # Arguments
            name: Name of the tag to be removed.

        # Raises
            `hsfs.client.exceptions.RestAPIError` in case the backend fails to delete the tag.
        """
        self._training_dataset_engine.delete_tag(self, name)

    def get_tag(self, name):
        """Get the tags of a training dataset.

        # Arguments
            name: Name of the tag to get.

        # Returns
            tag value

        # Raises
            `hsfs.client.exceptions.RestAPIError` in case the backend fails to retrieve the tag.
        """
        return self._training_dataset_engine.get_tag(self, name)

    def get_tags(self):
        """Returns all tags attached to a training dataset.

        # Returns
            `Dict[str, obj]` of tags.

        # Raises
            `hsfs.client.exceptions.RestAPIError` in case the backend fails to retrieve the tags.
        """
        return self._training_dataset_engine.get_tags(self)

    def update_statistics_config(self):
        """Update the statistics configuration of the training dataset.

        Change the `statistics_config` object and persist the changes by calling
        this method.

        # Returns
            `TrainingDataset`. The updated metadata object of the training dataset.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.
        """
        self._training_dataset_engine.update_statistics_config(self)
        return self

    def delete(self):
        """Delete training dataset and all associated metadata.

        !!! note "Drops only HopsFS data"
            Note that this operation drops only files which were materialized in
            HopsFS. If you used a Storage Connector for a cloud storage such as S3,
            the data will not be deleted, but you will not be able to track it anymore
            from the Feature Store.

        !!! danger "Potentially dangerous operation"
            This operation drops all metadata associated with **this version** of the
            training dataset **and** and the materialized data in HopsFS.

        # Raises
            `hsfs.client.exceptions.RestAPIError`.
        """
        warnings.warn(
            "All jobs associated to training dataset `{}`, version `{}` will be removed.".format(
                self._name, self._version
            ),
            util.JobWarning,
        )
        self._training_dataset_api.delete(self)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        for td in json_decamelized:
            _ = td.pop("type")
            cls._rewrite_location(td)
        return [cls(**td) for td in json_decamelized]

    @classmethod
    def from_response_json_single(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        json_decamelized.pop("type", None)
        json_decamelized.pop("href", None)
        cls._rewrite_location(json_decamelized)
        return cls(**json_decamelized)

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("type")
        # here we lose the information that the user set, e.g. write_options
        self._rewrite_location(json_decamelized)
        self.__init__(**json_decamelized)
        return self

    # A bug is introduced https://github.com/logicalclocks/hopsworks/blob/7adcad3cf5303ef19c996d75e6f4042cf565c8d5/hopsworks-common/src/main/java/io/hops/hopsworks/common/featurestore/trainingdatasets/hopsfs/HopsfsTrainingDatasetController.java#L85
    # Rewrite the td location if it is TD root directory
    @classmethod
    def _rewrite_location(cls, td_json):
        _client = client.get_instance()
        if "location" in td_json:
            if td_json["location"].endswith(
                f"/Projects/{_client._project_name}/{_client._project_name}_Training_Datasets"
            ):
                td_json[
                    "location"
                ] = f"{td_json['location']}/{td_json['name']}_{td_json['version']}"

    def _infer_training_dataset_type(self, connector_type):
        if connector_type == StorageConnector.HOPSFS or connector_type is None:
            return self.HOPSFS
        elif (
            connector_type == StorageConnector.S3
            or connector_type == StorageConnector.ADLS
            or connector_type == StorageConnector.GCS
        ):
            return self.EXTERNAL
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
            "coalesce": self._coalesce,
            "storageConnector": self._storage_connector,
            "location": self._location,
            "trainingDatasetType": self._training_dataset_type,
            "features": self._features,
            "splits": self._splits,
            "seed": self._seed,
            "queryDTO": self._querydto.to_dict() if self._querydto else None,
            "statisticsConfig": self._statistics_config,
            "trainSplit": self._train_split,
            "eventStartTime": self._start_time,
            "eventEndTime": self._end_time,
            "extraFilter": self._extra_filter,
            "type": "trainingDatasetDTO",
        }

    @property
    def id(self):
        """Training dataset id."""
        return self._id

    @id.setter
    def id(self, id):
        self._id = id

    @property
    def name(self):
        """Name of the training dataset."""
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def version(self):
        """Version number of the training dataset."""
        return self._version

    @version.setter
    def version(self, version):
        self._version = version

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, description):
        """Description of the training dataset contents."""
        self._description = description

    @property
    def data_format(self):
        """File format of the training dataset."""
        return self._data_format

    @data_format.setter
    def data_format(self, data_format):
        self._data_format = data_format

    @property
    def coalesce(self):
        """If true the training dataset data will be coalesced into
        a single partition before writing. The resulting training dataset
        will be a single file per split"""
        return self._coalesce

    @coalesce.setter
    def coalesce(self, coalesce):
        self._coalesce = coalesce

    @property
    def write_options(self):
        """User provided options to write training dataset."""
        return self._write_options

    @write_options.setter
    def write_options(self, write_options):
        self._write_options = write_options

    @property
    def storage_connector(self):
        """Storage connector."""
        return self._storage_connector

    @storage_connector.setter
    def storage_connector(self, storage_connector):
        if isinstance(storage_connector, StorageConnector):
            self._storage_connector = storage_connector
        elif storage_connector is None:
            # init empty connector, otherwise will have to handle it at serialization time
            self._storage_connector = HopsFSConnector(
                None, None, None, None, None, None
            )
        else:
            raise TypeError(
                "The argument `storage_connector` has to be `None` or of type `StorageConnector`, is of type: {}".format(
                    type(storage_connector)
                )
            )
        if self.training_dataset_type != self.IN_MEMORY:
            self._training_dataset_type = self._infer_training_dataset_type(
                self._storage_connector.type
            )

    @property
    def splits(self):
        """Training dataset splits. `train`, `test` or `eval` and corresponding percentages."""
        return self._splits

    @splits.setter
    def splits(self, splits):
        # user api differs from how the backend expects the splits to be represented
        self._splits = [
            TrainingDatasetSplit(
                name=k, split_type=TrainingDatasetSplit.RANDOM_SPLIT, percentage=v
            )
            for k, v in splits.items()
            if v is not None
        ]

    @property
    def location(self):
        """Path to the training dataset location."""
        return self._location

    @location.setter
    def location(self, location):
        self._location = location

    @property
    def schema(self):
        """Training dataset schema."""
        return self._features

    @schema.setter
    def schema(self, features):
        """Training dataset schema."""
        self._features = features

    @property
    def seed(self):
        """Seed."""
        return self._seed

    @seed.setter
    def seed(self, seed):
        self._seed = seed

    @property
    def statistics_config(self):
        """Statistics configuration object defining the settings for statistics
        computation of the training dataset."""
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
        """Get the latest computed statistics for the training dataset."""
        return self._statistics_engine.get_last(self)

    def get_statistics(self, commit_time: Union[str, int, datetime, date] = None):
        """Returns the statistics for this training dataset at a specific time.

        If `commit_time` is `None`, the most recent statistics are returned.

        # Arguments
            commit_time: If specified will recompute statistics on
                feature group as of specific point in time. If not specified then will compute statistics
                as of most recent time of this feature group. Defaults to `None`. Strings should
                be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`.

        # Returns
            `Statistics`. Object with statistics information.
        """
        if commit_time is None:
            return self.statistics
        else:
            return self._statistics_engine.get(self, commit_time)

    @property
    def query(self):
        """Query to generate this training dataset from online feature store."""
        return self._training_dataset_engine.query(self, True, True, False)

    def get_query(self, online: bool = True, with_label: bool = False):
        """Returns the query used to generate this training dataset

        # Arguments
            online: boolean, optional. Return the query for the online storage, else
                for offline storage, defaults to `True` - for online storage.
            with_label: Indicator whether the query should contain features which were
                marked as prediction label/feature when the training dataset was
                created, defaults to `False`.

        # Returns
            `str`. Query string for the chosen storage used to generate this training
                dataset.
        """
        return self._training_dataset_engine.query(
            self, online, with_label, engine.get_type() == "python"
        )

    def init_prepared_statement(
        self, batch: Optional[bool] = None, external: Optional[bool] = None
    ):
        """Initialise and cache parametrized prepared statement to
           retrieve feature vector from online feature store.

        # Arguments
            batch: boolean, optional. If set to True, prepared statements will be
                initialised for retrieving serving vectors as a batch.
            external: boolean, optional. If set to True, the connection to the
                online feature store is established using the same host as
                for the `host` parameter in the [`hsfs.connection()`](connection_api.md#connection) method.
                If set to False, the online feature store storage connector is used
                which relies on the private IP. Defaults to True if connection to Hopsworks is established from
                external environment (e.g AWS Sagemaker or Google Colab), otherwise to False.
        """
        self._vector_server.init_serving(self, batch, external)

    def get_serving_vector(
        self, entry: Dict[str, Any], external: Optional[bool] = None
    ):
        """Returns assembled serving vector from online feature store.

        # Arguments
            entry: dictionary of training dataset feature group primary key names as keys and values provided by
                serving application.
            external: boolean, optional. If set to True, the connection to the
                online feature store is established using the same host as
                for the `host` parameter in the [`hsfs.connection()`](connection_api.md#connection) method.
                If set to False, the online feature store storage connector is used
                which relies on the private IP. Defaults to True if connection to Hopsworks is established from
                external environment (e.g AWS Sagemaker or Google Colab), otherwise to False.
        # Returns
            `list` List of feature values related to provided primary keys, ordered according to positions of this
            features in training dataset query.
        """
        if self._vector_server.prepared_statements is None:
            self.init_prepared_statement(None, external)
        return self._vector_server.get_feature_vector(entry)

    def get_serving_vectors(
        self, entry: Dict[str, List[Any]], external: Optional[bool] = None
    ):
        """Returns assembled serving vectors in batches from online feature store.

        # Arguments
            entry: dict of feature group primary key names as keys and value as list of primary keys provided by
                serving application.
            external: boolean, optional. If set to True, the connection to the
                online feature store is established using the same host as
                for the `host` parameter in the [`hsfs.connection()`](connection_api.md#connection) method.
                If set to False, the online feature store storage connector is used
                which relies on the private IP. Defaults to True if connection to Hopsworks is established from
                external environment (e.g AWS Sagemaker or Google Colab), otherwise to False.
        # Returns
            `List[list]` List of lists of feature values related to provided primary keys, ordered according to
            positions of this features in training dataset query.
        """
        if self._vector_server.prepared_statements is None:
            self.init_prepared_statement(None, external)
        return self._vector_server.get_feature_vectors(entry)

    @property
    def label(self):
        """The label/prediction feature of the training dataset.

        Can be a composite of multiple features.
        """
        return self._label

    @label.setter
    def label(self, label):
        self._label = [lb.lower() for lb in label]

    @property
    def feature_store_id(self):
        return self._feature_store_id

    @property
    def feature_store_name(self):
        """Name of the feature store in which the feature group is located."""
        return self._feature_store_name

    @property
    def train_split(self):
        """Set name of training dataset split that is used for training."""
        return self._train_split

    @train_split.setter
    def train_split(self, train_split):
        self._train_split = train_split

    @property
    def transformation_functions(self):
        """Set transformation functions."""
        if self._id is not None and self._transformation_functions is None:
            self._transformation_functions = (
                self._transformation_function_engine.get_td_transformation_fn(self)
            )
        return self._transformation_functions

    @transformation_functions.setter
    def transformation_functions(self, transformation_functions):
        self._transformation_functions = transformation_functions

    @property
    def event_start_time(self):
        return self._start_time

    @event_start_time.setter
    def event_start_time(self, start_time):
        self._start_time = start_time

    @property
    def event_end_time(self):
        return self._end_time

    @event_end_time.setter
    def event_end_time(self, end_time):
        self._end_time = end_time

    @property
    def training_dataset_type(self):
        return self._training_dataset_type

    @training_dataset_type.setter
    def training_dataset_type(self, training_dataset_type):
        valid_type = [self.IN_MEMORY, self.HOPSFS, self.EXTERNAL]
        if training_dataset_type not in valid_type:
            raise ValueError(
                "Training dataset type should be one of " ", ".join(valid_type)
            )
        else:
            self._training_dataset_type = training_dataset_type

    @property
    def validation_size(self):
        return self._validation_size

    @validation_size.setter
    def validation_size(self, validation_size):
        self._validation_size = validation_size

    @property
    def test_size(self):
        return self._test_size

    @test_size.setter
    def test_size(self, test_size):
        self._test_size = test_size

    @property
    def train_start(self):
        return self._train_start

    @train_start.setter
    def train_start(self, train_start):
        self._train_start = train_start

    @property
    def train_end(self):
        return self._train_end

    @train_end.setter
    def train_end(self, train_end):
        self._train_end = train_end

    @property
    def validation_start(self):
        return self._validation_start

    @validation_start.setter
    def validation_start(self, validation_start):
        self._validation_start = validation_start

    @property
    def validation_end(self):
        return self._validation_end

    @validation_end.setter
    def validation_end(self, validation_end):
        self._validation_end = validation_end

    @property
    def test_start(self):
        return self._test_start

    @test_start.setter
    def test_start(self, test_start):
        self._test_start = test_start

    @property
    def test_end(self):
        return self._test_end

    @test_end.setter
    def test_end(self, test_end):
        self._test_end = test_end

    def serving_keys(self):
        """Set of primary key names that is used as keys in input dict object for `get_serving_vector` method."""
        if self._vector_server.serving_keys:
            return self._vector_server.serving_keys
        else:
            _vector_server = vector_server.VectorServer(
                self._feature_store_id, self._features
            )
            _vector_server.init_prepared_statement(self, False, False)
            return _vector_server.serving_keys

    @property
    def extra_filter(self):
        return self._extra_filter

    @extra_filter.setter
    def extra_filter(self, extra_filter):
        self._extra_filter = extra_filter
