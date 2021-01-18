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

from hsfs import engine, training_dataset_feature
from hsfs.core import training_dataset_api, tags_api
from hsfs.constructor import query


class TrainingDatasetEngine:
    OVERWRITE = "overwrite"
    APPEND = "append"
    ENTITY_TYPE = "trainingdatasets"

    def __init__(self, feature_store_id):
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

        self._training_dataset_api.post(training_dataset)

        engine.get_instance().write_training_dataset(
            training_dataset, features, user_write_options, self.OVERWRITE
        )

    def insert(self, training_dataset, features, user_write_options, overwrite):
        # validate matching schema
        if engine.get_type() == "spark":
            if isinstance(features, query.Query):
                dataframe = features.read()
            else:
                dataframe = features

            engine.get_instance().training_dataset_schema_match(
                dataframe, training_dataset.schema
            )

        engine.get_instance().write_training_dataset(
            training_dataset,
            features,
            user_write_options,
            self.OVERWRITE if overwrite else self.APPEND,
        )

    def read(self, training_dataset, split, user_read_options):
        if split is None:
            path = training_dataset.location + "/" + "**"
        else:
            path = training_dataset.location + "/" + str(split)

        read_options = engine.get_instance().read_options(
            training_dataset.data_format, user_read_options
        )

        return engine.get_instance().read(
            training_dataset.storage_connector,
            training_dataset.data_format,
            read_options,
            path,
        )

    def query(self, training_dataset, online, with_label):
        return self._training_dataset_api.get_query(training_dataset, with_label)[
            "queryOnline" if online else "query"
        ]

    def add_tag(self, training_dataset, name, value):
        """Attach a name/value tag to a training dataset."""
        self._tags_api.add(training_dataset, name, value)

    def delete_tag(self, training_dataset, name):
        """Remove a tag from a training dataset."""
        self._tags_api.delete(training_dataset, name)

    def get_tags(self, training_dataset, name):
        """Get tag with a certain name or all tags for a training dataset."""
        return [tag.to_dict() for tag in self._tags_api.get(training_dataset, name)]

    def update_statistics_config(self, training_dataset):
        """Update the statistics configuration of a feature group."""
        self._training_dataset_api.update_metadata(
            training_dataset, training_dataset, "updateStatsConfig"
        )
