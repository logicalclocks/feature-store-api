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

import warnings
import humps
from typing import Optional, Union, List, Dict

from hsfs import (
    training_dataset,
    feature_group,
    feature,
    on_demand_feature_group,
    util,
    storage_connector,
)
from hsfs.core import (
    feature_group_api,
    storage_connector_api,
    training_dataset_api,
    feature_group_engine,
)
from hsfs.statistics_config import StatisticsConfig


class FeatureStore:
    DEFAULT_VERSION = 1

    def __init__(
        self,
        featurestore_id,
        featurestore_name,
        created,
        hdfs_store_path,
        project_name,
        project_id,
        featurestore_description,
        inode_id,
        offline_featurestore_name,
        hive_endpoint,
        online_enabled,
        online_featurestore_name=None,
        mysql_server_endpoint=None,
        online_featurestore_size=None,
    ):
        self._id = featurestore_id
        self._name = featurestore_name
        self._created = created
        self._hdfs_store_path = hdfs_store_path
        self._project_name = project_name
        self._project_id = project_id
        self._description = featurestore_description
        self._inode_id = inode_id
        self._online_feature_store_name = online_featurestore_name
        self._online_feature_store_size = online_featurestore_size
        self._offline_feature_store_name = offline_featurestore_name
        self._hive_endpoint = hive_endpoint
        self._mysql_server_endpoint = mysql_server_endpoint
        self._online_enabled = online_enabled

        self._feature_group_api = feature_group_api.FeatureGroupApi(self._id)
        self._storage_connector_api = storage_connector_api.StorageConnectorApi(
            self._id
        )
        self._training_dataset_api = training_dataset_api.TrainingDatasetApi(self._id)

        self._feature_group_engine = feature_group_engine.FeatureGroupEngine(self._id)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def get_feature_group(self, name: str, version: int = None):
        """Get a feature group entity from the feature store.

        Getting a feature group from the Feature Store means getting its metadata handle
        so you can subsequently read the data into a Spark or Pandas DataFrame or use
        the `Query`-API to perform joins between feature groups.

        # Arguments
            name: Name of the feature group to get.
            version: Version of the feature group to retrieve, defaults to `None` and will
                return the `version=1`.

        # Returns
            `FeatureGroup`: The feature group metadata object.

        # Raises
            `RestAPIError`: If unable to retrieve feature group from the feature store.

        """
        if version is None:
            warnings.warn(
                "No version provided for getting feature group `{}`, defaulting to `{}`.".format(
                    name, self.DEFAULT_VERSION
                ),
                util.VersionWarning,
            )
            version = self.DEFAULT_VERSION
        return self._feature_group_api.get(
            name, version, feature_group_api.FeatureGroupApi.CACHED
        )

    def get_on_demand_feature_group(self, name: str, version: int = None):
        """Get a on-demand feature group entity from the feature store.

        Getting a on-demand feature group from the Feature Store means getting its
        metadata handle so you can subsequently read the data into a Spark or
        Pandas DataFrame or use the `Query`-API to perform joins between feature groups.

        # Arguments
            name: Name of the on-demand feature group to get.
            version: Version of the on-demand feature group to retrieve,
                defaults to `None` and will return the `version=1`.

        # Returns
            `OnDemandFeatureGroup`: The on-demand feature group metadata object.

        # Raises
            `RestAPIError`: If unable to retrieve feature group from the feature store.

        """

        if version is None:
            warnings.warn(
                "No version provided for getting feature group `{}`, defaulting to `{}`.".format(
                    name, self.DEFAULT_VERSION
                ),
                util.VersionWarning,
            )
            version = self.DEFAULT_VERSION
        return self._feature_group_api.get(
            name, version, feature_group_api.FeatureGroupApi.ONDEMAND
        )

    def get_training_dataset(self, name: str, version: int = None):
        """Get a training dataset entity from the feature store.

        Getting a training dataset from the Feature Store means getting its metadata handle
        so you can subsequently read the data into a Spark or Pandas DataFrame.

        # Arguments
            name: Name of the training dataset to get.
            version: Version of the training dataset to retrieve, defaults to `None` and will
                return the `version=1`.

        # Returns
            `TrainingDataset`: The training dataset metadata object.

        # Raises
            `RestAPIError`: If unable to retrieve feature group from the feature store.
        """

        if version is None:
            warnings.warn(
                "No version provided for getting training dataset `{}`, defaulting to `{}`.".format(
                    name, self.DEFAULT_VERSION
                ),
                util.VersionWarning,
            )
            version = self.DEFAULT_VERSION
        return self._training_dataset_api.get(name, version)

    def get_storage_connector(self, name: str, connector_type: str):
        """Get a previously created storage connector from the feature store.

        Storage connectors encapsulate all information needed for the execution engine
        to read and write to specific storage. This storage can be S3, a JDBC compliant
        database or the distributed filesystem HOPSFS.

        If you want to connect to the online feature store, see the
        `get_online_storage_connector` method to get the JDBC connector for the Online
        Feature Store.

        !!! example "Getting a Storage Connector"
            ```python

            sc = fs.get_storage_connector("demo_fs_meb10000_Training_Datasets", "HOPSFS")

            td = fs.create_training_dataset(..., storage_connector=sc, ...)
            ```

        # Arguments
            name: Name of the storage connector to retrieve.
            connector_type: Type of the storage connector, e.g. `"JDBC"`, `"HOPSFS"`
                or `"S3"`.

        # Returns
            `StorageConnector`. Storage connector object.
        """
        return self._storage_connector_api.get(name, connector_type)

    def sql(self, query, dataframe_type="default", online=False):
        return self._feature_group_engine.sql(query, self._name, dataframe_type, online)

    def get_online_storage_connector(self):
        """Get the storage connector for the Online Feature Store of the respective
        project's feature store.

        The returned storage connector depends on the project that you are connected to.

        # Returns
            `StorageConnector`. JDBC storage connector to the Online Feature Store.
        """
        return self._storage_connector_api.get_online_connector()

    def create_feature_group(
        self,
        name: str,
        version: Optional[int] = None,
        description: Optional[str] = "",
        online_enabled: Optional[bool] = False,
        time_travel_format: Optional[str] = "HUDI",
        partition_key: Optional[List[str]] = [],
        primary_key: Optional[List[str]] = [],
        features: Optional[List[feature.Feature]] = [],
        statistics_config: Optional[Union[StatisticsConfig, bool, dict]] = None,
    ):
        """Create a feature group metadata object.

        !!! note "Lazy"
            This method is lazy and does not persist any metadata or feature data in the
            feature store on its own. To persist the feature group and save feature data
            along the metadata in the feature store, call the `save()` method with a
            DataFrame.

        # Arguments
            name: Name of the feature group to create.
            version: Version of the feature group to retrieve, defaults to `None` and
                will create the feature group with incremented version from the last
                version in the feature store.
            description: A string describing the contents of the feature group to
                improve discoverability for Data Scientists, defaults to empty string
                `""`.
            online_enabled: Define whether the feature group should be made available
                also in the online feature store for low latency access, defaults to
                `False`.
            time_travel_format: Format used for time travel, defaults to `"HUDI"`.
            partition_key: A list of feature names to be used as partition key when
                writing the feature data to the offline storage, defaults to empty list
                `[]`.
            primary_key: A list of feature names to be used as primary key for the
                feature group. This primary key can be a composite key of multiple
                features and will be used as joining key, if not specified otherwise.
                Defaults to empty list `[]`, and the first column of the DataFrame will
                be used as primary key.
            features: Optionally, define the schema of the feature group manually as a
                list of `Feature` objects. Defaults to empty list `[]` and will use the
                schema information of the DataFrame provided in the `save` method.
            statistics_config: A configuration object, or a dictionary with keys
                "`enabled`" to generally enable descriptive statistics computation for
                this feature group, `"correlations`" to turn on feature correlation
                computation and `"histograms"` to compute feature value frequencies. The
                values should be booleans indicating the setting. To fully turn off
                statistics computation pass `statistics_config=False`. Defaults to
                `None` and will compute only descriptive statistics.

        # Returns
            `FeatureGroup`. The feature group metadata object.
        """
        return feature_group.FeatureGroup(
            name=name,
            version=version,
            description=description,
            online_enabled=online_enabled,
            time_travel_format=time_travel_format,
            partition_key=partition_key,
            primary_key=primary_key,
            featurestore_id=self._id,
            featurestore_name=self._name,
            features=features,
            statistics_config=statistics_config,
        )

    def create_on_demand_feature_group(
        self,
        name: str,
        query: str,
        storage_connector: storage_connector.StorageConnector,
        version: Optional[int] = None,
        description: Optional[str] = "",
        features: Optional[List[feature.Feature]] = [],
    ):
        """Create a on-demand feature group metadata object.

        !!! note "Lazy"
            This method is lazy and does not persist any metadata or feature data in the
            feature store on its own. To persist the feature group and save feature data
            along the metadata in the feature store, call the `save()` method.

        # Arguments
            name: Name of the on-demand feature group to create.
            query: A string containing a SQL query valid for the target data source.
                the query will be used to pull data from the data sources when the
                feature group is used.
            storage_connector: the storage connector to use to establish connectivity
                with the data source.
            version: Version of the on-demand feature group to retrieve, defaults to `None` and
                will create the feature group with incremented version from the last
                version in the feature store.
            description: A string describing the contents of the on-demand feature group to
                improve discoverability for Data Scientists, defaults to empty string
                `""`.
            features: Optionally, define the schema of the on-demand feature group manually as a
                list of `Feature` objects. Defaults to empty list `[]` and will use the
                schema information of the DataFrame resulting by executing the provided query
                against the data source.
        """
        return on_demand_feature_group.OnDemandFeatureGroup(
            name=name,
            query=query,
            storage_connector=storage_connector,
            version=version,
            description=description,
            featurestore_id=self._id,
            featurestore_name=self._name,
            features=features,
        )

    def create_training_dataset(
        self,
        name: str,
        version: Optional[int] = None,
        description: Optional[str] = "",
        data_format: Optional[str] = "tfrecords",
        storage_connector: Optional[storage_connector.StorageConnector] = None,
        splits: Optional[Dict[str, float]] = {},
        location: Optional[str] = "",
        seed: Optional[int] = None,
        statistics_config: Optional[Union[StatisticsConfig, bool, dict]] = None,
        label: Optional[List[str]] = [],
    ):
        """Create a training dataset metadata object.

        !!! note "Lazy"
            This method is lazy and does not persist any metadata or feature data in the
            feature store on its own. To materialize the training dataset and save
            feature data along the metadata in the feature store, call the `save()`
            method with a `DataFrame` or `Query`.

        !!! info "Data Formats"
            The feature store currently supports the following data formats for
            training datasets:

            1. tfrecord
            2. csv
            3. tsv
            4. parquet
            5. avro
            6. orc

            Currently not supported petastorm, hdf5 and npy file formats.


        # Arguments
            name: Name of the training dataset to create.
            version: Version of the training dataset to retrieve, defaults to `None` and
                will create the training dataset with incremented version from the last
                version in the feature store.
            description: A string describing the contents of the training dataset to
                improve discoverability for Data Scientists, defaults to empty string
                `""`.
            data_format: The data format used to save the training dataset,
                defaults to `"tfrecords"`-format.
            storage_connector: Storage connector defining the sink location for the
                training dataset, defaults to `None`, and materializes training dataset
                on HopsFS.
            splits: A dictionary defining training dataset splits to be created. Keys in
                the dictionary define the name of the split as `str`, values represent
                percentage of samples in the split as `float`. Currently, only random
                splits are supported. Defaults to empty dict`{}`, creating only a single
                training dataset without splits.
            location: Path to complement the sink storage connector with, e.g if the
                storage connector points to an S3 bucket, this path can be used to
                define a sub-directory inside the bucket to place the training dataset.
                Defaults to `""`, saving the training dataset at the root defined by the
                storage connector.
            seed: Optionally, define a seed to create the random splits with, in order
                to guarantee reproducability, defaults to `None`.
            statistics_config: A configuration object, or a dictionary with keys
                "`enabled`" to generally enable descriptive statistics computation for
                this feature group, `"correlations`" to turn on feature correlation
                computation and `"histograms"` to compute feature value frequencies. The
                values should be booleans indicating the setting. To fully turn off
                statistics computation pass `statistics_config=False`. Defaults to
                `None` and will compute only descriptive statistics.
            label: A list of feature names constituting the prediction label/feature of
                the training dataset. When replaying a `Query` during model inference,
                the label features can be omitted from the feature vector retrieval.
                Defaults to `[]`, no label.

        # Returns:
            `TrainingDataset`: The training dataset metadata object.
        """
        return training_dataset.TrainingDataset(
            name=name,
            version=version,
            description=description,
            data_format=data_format,
            storage_connector=storage_connector,
            location=location,
            featurestore_id=self._id,
            splits=splits,
            seed=seed,
            statistics_config=statistics_config,
            label=label,
        )

    @property
    def id(self):
        """Id of the feature store."""
        return self._id

    @property
    def name(self):
        """Name of the feature store."""
        return self._name

    @property
    def project_name(self):
        """Name of the project in which the feature store is located."""
        return self._project_name

    @property
    def project_id(self):
        """Id of the project in which the feature store is located."""
        return self._project_id

    @property
    def description(self):
        """Description of the feature store."""
        return self._description

    @property
    def online_featurestore_name(self):
        """Name of the online feature store database."""
        return self._online_feature_store_name

    @property
    def mysql_server_endpoint(self):
        """MySQL server endpoint for the online feature store."""
        return self._mysql_server_endpoint

    @property
    def online_enabled(self):
        """Indicator whether online feature store is enabled."""
        return self._online_enabled

    @property
    def hive_endpoint(self):
        """Hive endpoint for the offline feature store."""
        return self._hive_endpoint

    @property
    def offline_featurestore_name(self):
        """Name of the offline feature store database."""
        return self._offline_feature_store_name
