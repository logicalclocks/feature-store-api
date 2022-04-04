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

from hsfs import engine, training_dataset_feature

from hsfs.core import (
    tags_api,
    storage_connector_api,
    transformation_function_engine,
    feature_view_api
)


class FeatureViewEngine:
    ENTITY_TYPE = "featureview"

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
