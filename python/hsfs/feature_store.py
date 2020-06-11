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

from hsfs import engine, training_dataset, feature_group
from hsfs.core import (
    feature_group_api,
    storage_connector_api,
    training_dataset_api,
)


class FeatureStore:
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
        mysql_server_endpoint,
        online_enabled,
        online_featurestore_name=None,
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

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def get_feature_group(self, name, version):
        return self._feature_group_api.get(name, version)

    def get_training_dataset(self, name, version):
        return self._training_dataset_api.get(name, version)

    def get_storage_connector(self, name, connector_type):
        return self._storage_connector_api.get(name, connector_type)

    def sql(self, query, dataframe_type="default"):
        return engine.get_instance().sql(query, self._name, dataframe_type)

    def create_feature_group(
        self,
        name,
        version,
        description="",
        default_storage="offline",
        online_enabled=False,
        partition_key=[],
        primary_key=[],
        features=[],
    ):
        return feature_group.FeatureGroup(
            name=name,
            version=version,
            description=description,
            online_enabled=online_enabled,
            default_storage=default_storage,
            partition_key=partition_key,
            primary_key=primary_key,
            featurestore_id=self._id,
            featurestore_name=self._name,
            featuregroup_type=feature_group.FeatureGroup.CACHED_FEATURE_GROUP,
            features=features,
        )

    def create_training_dataset(
        self,
        name,
        version,
        description="",
        data_format="tfrecords",
        storage_connector=None,
        splits={},
        location="",
        seed=None,
    ):
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
        )
