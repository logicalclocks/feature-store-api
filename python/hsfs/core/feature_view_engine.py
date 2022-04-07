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

import warnings
from hsfs import engine, training_dataset_feature

from hsfs.core import (
    tags_api,
    storage_connector_api,
    transformation_function_engine,
    feature_view_api,
    code_engine
)


class FeatureViewEngine:
    ENTITY_TYPE = "featureview"
    _OVERWRITE = "overwrite"
    _APPEND = "append"

    def __init__(self, feature_store_id):
        self._feature_store_id = feature_store_id

        self._feature_view_api = feature_view_api.FeatureViewApi(
            feature_store_id
        )
        self._tags_api = tags_api.TagsApi(feature_store_id, self.ENTITY_TYPE)
        self._storage_connector_api = storage_connector_api.StorageConnectorApi(
            feature_store_id
        )
        self._transformation_function_engine = (
            transformation_function_engine.TransformationFunctionEngine(
                feature_store_id
            )
        )
        self._td_code_engine = code_engine.CodeEngine(feature_store_id,
                                                   "trainingdatasets")

    def save(self, feature_view_obj):
        if feature_view_obj.label:
            feature_view_obj._features = [
                training_dataset_feature.TrainingDatasetFeature(
                    name=label_name, label=True
                )
                for label_name in feature_view_obj.label
            ]
        self._transformation_function_engine.attach_transformation_fn(
            feature_view_obj
        )
        return self._feature_view_api.post(feature_view_obj)

    def get(self, name, version=None):
        if version:
            return self._feature_view_api.get_by_name_version(name, version)
        else:
            return self._feature_view_api.get_by_name(name)

    def delete(self, name, version=None):
        if version:
            return self._feature_view_api.delete_by_name_version(name, version)
        else:
            return self._feature_view_api.delete_by_name(name)

    def get_batch_query(self, feature_view_obj, start_time, end_time):
        return self._feature_view_api.get_batch_query(
            feature_view_obj.name, feature_view_obj.version, start_time,
            end_time, is_python_engine=engine.get_type() == "python")

    def get_attached_transformation_fn(self, name, version):
        return self._feature_view_api.get_attached_transformation_fn(
            name, version
        )

    def create_training_dataset(self, feature_view_obj,
                                training_dataset_obj, user_write_options):
        if (len(training_dataset_obj.splits) > 0 and
            training_dataset_obj.train_split is None):
            training_dataset_obj.train_split = "train"
            warnings.warn(
                "Training dataset splits were defined but no `train_split` (the name of the split that is going to be "
                "used for training) was provided. Setting this property to `train`. The statistics of this "
                "split will be used for transformation functions."
            )

        updated_instance = self._feature_view_api.create_training_dataset(
            feature_view_obj.name, feature_view_obj.version,
            training_dataset_obj)
        td_job = self.compute_training_dataset(
            feature_view_obj,
            user_write_options,
            training_dataset_obj=training_dataset_obj
        )
        return updated_instance, td_job

    # This method is used by hsfs_utils to launch a job for python client
    def compute_training_dataset(self, feature_view_obj, user_write_options,
                                 training_dataset_obj=None,
                                 training_dataset_version=None):
        if training_dataset_obj:
            pass
        elif training_dataset_version:
            training_dataset_obj = \
                self._feature_view_api.get_training_dataset_by_version(
                    feature_view_obj.name, feature_view_obj.version,
                    training_dataset_version)
        else:
            raise ValueError("No training dataset object or version is provided")

        training_dataset_obj.schema = feature_view_obj.schema
        td_job = engine.get_instance().write_training_dataset(
            training_dataset_obj, feature_view_obj.query, user_write_options,
            self._OVERWRITE, feature_view_obj=feature_view_obj
        )
        # currently we do not save the training dataset statistics config for training datasets
        self._td_code_engine.save_code(training_dataset_obj)
        if training_dataset_obj.statistics_config.enabled and engine.get_type() == "spark":
            self.compute_training_dataset_statistics(
                self, training_dataset_obj)
        return td_job

    def compute_training_dataset_statistics(self, feature_view_obj,
                                            training_dataset_obj):
        pass
