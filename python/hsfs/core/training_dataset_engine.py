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

from hsfs import engine
from hsfs.core import training_dataset_api, tags_api


class TrainingDatasetEngine:
    OVERWRITE = "overwrite"
    APPEND = "append"
    ENTITY_TYPE = "trainingdatasets"

    def __init__(self, feature_store_id):
        self._training_dataset_api = training_dataset_api.TrainingDatasetApi(
            feature_store_id
        )
        self._tags_api = tags_api.TagsApi(feature_store_id, self.ENTITY_TYPE)

    def save(self, training_dataset, feature_dataframe, user_write_options):
        self._training_dataset_api.post(training_dataset)

        write_options = engine.get_instance().write_options(
            training_dataset.data_format, user_write_options
        )

        self._write(training_dataset, feature_dataframe, write_options, self.OVERWRITE)

    def insert(
        self, training_dataset, feature_dataframe, user_write_options, overwrite
    ):
        # validate matching schema
        engine.get_instance().training_dataset_schema_match(
            feature_dataframe, training_dataset.schema
        )

        write_options = engine.get_instance().write_options(
            training_dataset.data_format, user_write_options
        )

        self._write(
            training_dataset,
            feature_dataframe,
            write_options,
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

    def _write(self, training_dataset, dataset, write_options, save_mode):
        if len(training_dataset.splits) == 0:
            path = training_dataset.location + "/" + training_dataset.name
            self._write_single(
                dataset,
                training_dataset.storage_connector,
                training_dataset.data_format,
                write_options,
                save_mode,
                path,
            )
        else:
            split_names = sorted([*training_dataset.splits])
            split_weights = [training_dataset.splits[i] for i in split_names]
            self._write_splits(
                dataset.randomSplit(split_weights, training_dataset.seed),
                training_dataset.storage_connector,
                training_dataset.data_format,
                write_options,
                save_mode,
                training_dataset.location,
                split_names,
            )

    def _write_splits(
        self,
        feature_dataframe_list,
        storage_connector,
        data_format,
        write_options,
        save_mode,
        path,
        split_names,
    ):
        for i in range(len(feature_dataframe_list)):
            split_path = path + "/" + str(split_names[i])
            self._write_single(
                feature_dataframe_list[i],
                storage_connector,
                data_format,
                write_options,
                save_mode,
                split_path,
            )

    def _write_single(
        self,
        feature_dataframe,
        storage_connector,
        data_format,
        write_options,
        save_mode,
        path,
    ):
        # TODO: currently not supported petastorm, hdf5 and npy file formats
        engine.get_instance().write(
            feature_dataframe,
            storage_connector,
            data_format,
            save_mode,
            write_options,
            path,
        )

    def add_tag(self, training_dataset, name, value):
        """Attach a name/value tag to a training dataset."""
        self._tags_api.add(training_dataset, name, value)

    def delete_tag(self, training_dataset, name):
        """Remove a tag from a training dataset."""
        self._tags_api.delete(training_dataset, name)

    def get_tags(self, training_dataset, name):
        """Get tag with a certain name or all tags for a training dataset."""
        return [tag.to_dict() for tag in self._tags_api.get(training_dataset, name)]
