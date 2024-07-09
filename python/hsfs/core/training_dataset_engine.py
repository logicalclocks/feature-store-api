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
from __future__ import annotations

import warnings

from hsfs import engine, training_dataset_feature
from hsfs.constructor import query
from hsfs.core import (
    tags_api,
    training_dataset_api,
)


class TrainingDatasetEngine:
    OVERWRITE = "overwrite"
    APPEND = "append"
    ENTITY_TYPE = "trainingdatasets"

    def __init__(self, feature_store_id):
        self._feature_store_id = feature_store_id

        self._training_dataset_api = training_dataset_api.TrainingDatasetApi(
            feature_store_id
        )
        self._tags_api = tags_api.TagsApi(feature_store_id, self.ENTITY_TYPE)

    def save(self, training_dataset, features, user_write_options):
        if isinstance(features, query.Query):
            training_dataset._querydto = features
            training_dataset._features = [
                training_dataset_feature.TrainingDatasetFeature(
                    name=label_name, label=True
                )
                for label_name in training_dataset.label
            ]
        else:
            features = engine.get_instance().convert_to_default_dataframe(features)
            training_dataset._features = (
                engine.get_instance().parse_schema_training_dataset(features)
            )
            for label_name in training_dataset.label:
                for feature in training_dataset._features:
                    if feature.name == label_name:
                        feature.label = True

        if len(training_dataset.splits) > 0 and training_dataset.train_split is None:
            training_dataset.train_split = "train"
            warnings.warn(
                "Training dataset splits were defined but no `train_split` (the name of the split that is going to be "
                "used for training) was provided. Setting this property to `train`. ",
                stacklevel=1,
            )

        updated_instance = self._training_dataset_api.post(training_dataset)
        td_job = engine.get_instance().write_training_dataset(
            training_dataset, features, user_write_options, self.OVERWRITE
        )
        return updated_instance, td_job

    def insert(self, training_dataset, dataset, user_write_options, overwrite):
        return engine.get_instance().write_training_dataset(
            training_dataset,
            dataset,
            user_write_options,
            self.OVERWRITE if overwrite else self.APPEND,
        )

    def read(self, training_dataset, split, user_read_options):
        read_options = engine.get_instance().read_options(
            training_dataset.data_format, user_read_options
        )

        if split is not None:
            path = training_dataset.location + "/" + str(split)
        else:
            path = training_dataset.location + "/" + training_dataset.name

        return training_dataset.storage_connector.read(
            # always read from materialized dataset, not query object
            query=None,
            data_format=training_dataset.data_format,
            options=read_options,
            path=path,
        )

    def query(self, training_dataset, online, with_label, is_hive_query):
        fs_query = self._training_dataset_api.get_query(
            training_dataset, with_label, is_hive_query
        )

        if online:
            return fs_query.query_online

        # The offline queries could be referencing temporary tables
        # like external feature groups/hudi feature groups
        # Here we register those tables before returning the query to the user
        # In this way, if they execute the query, it will be valid
        fs_query.register_external()
        fs_query.register_hudi_tables(
            self._feature_store_id,
            None,  # No need to provide the feature store name for read operations
            {},
        )
        if fs_query.pit_query is not None:
            return fs_query.pit_query

        return fs_query.query

    def add_tag(self, training_dataset, name, value):
        """Attach a name/value tag to a training dataset."""
        self._tags_api.add(training_dataset, name, value)

    def delete_tag(self, training_dataset, name):
        """Remove a tag from a training dataset."""
        self._tags_api.delete(training_dataset, name)

    def get_tag(self, training_dataset, name):
        """Get tag with a certain name for a training dataset."""
        return self._tags_api.get(training_dataset, name)[name]

    def get_tags(self, training_dataset):
        """Get all tags for a training dataset."""
        return self._tags_api.get(training_dataset)

    def update_statistics_config(self, training_dataset):
        """Update the statistics configuration of a feature group."""
        self._training_dataset_api.update_metadata(
            training_dataset, training_dataset, "updateStatsConfig"
        )
