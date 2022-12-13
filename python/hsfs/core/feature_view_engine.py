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

import datetime
import warnings
from hsfs import engine, training_dataset_feature, client, util
from hsfs.client import exceptions
from hsfs.client.exceptions import FeatureStoreException
from hsfs.training_dataset_split import TrainingDatasetSplit
from hsfs.core import (
    tags_api,
    storage_connector_api,
    transformation_function_engine,
    feature_view_api,
    code_engine,
    statistics_engine,
    training_dataset_engine,
    query_constructor_api,
)


class FeatureViewEngine:
    ENTITY_TYPE = "featureview"
    _TRAINING_DATA_API_PATH = "trainingdatasets"
    _OVERWRITE = "overwrite"
    _APPEND = "append"
    AMBIGUOUS_LABEL_ERROR = (
        "Provided label '{}' is ambiguous and exists in more than one feature groups. "
        "You can provide the label with the prefix you specify in the join."
    )
    LABEL_NOT_EXIST_ERROR = (
        "Provided label '{}' do not exist in any of the feature groups."
    )

    def __init__(self, feature_store_id):
        self._feature_store_id = feature_store_id

        self._feature_view_api = feature_view_api.FeatureViewApi(feature_store_id)
        self._tags_api = tags_api.TagsApi(feature_store_id, self.ENTITY_TYPE)
        self._storage_connector_api = storage_connector_api.StorageConnectorApi(
            feature_store_id
        )
        self._transformation_function_engine = (
            transformation_function_engine.TransformationFunctionEngine(
                feature_store_id
            )
        )
        self._td_code_engine = code_engine.CodeEngine(
            feature_store_id, self._TRAINING_DATA_API_PATH
        )
        self._statistics_engine = statistics_engine.StatisticsEngine(
            feature_store_id, self._TRAINING_DATA_API_PATH
        )
        self._training_dataset_engine = training_dataset_engine.TrainingDatasetEngine(
            feature_store_id
        )
        self._query_constructor_api = query_constructor_api.QueryConstructorApi()

    def save(self, feature_view_obj):
        if feature_view_obj.query.is_time_travel():
            warnings.warn(
                "`as_of` argument in the `Query` will be ignored because"
                " feature view does not support time travel query."
            )
        if feature_view_obj.labels:
            # If provided label matches column with prefix, then attach label.
            # If provided label matches only one column without prefix, then attach label. (For
            # backward compatibility purpose, as of v3.0, labels are matched to columns without prefix.)
            # If provided label matches multiple columns without prefix, then raise exception because it is ambiguous.
            prefix_feature_map = {}
            feature_map = {}
            for feat in feature_view_obj.query.features:
                prefix_feature_map[feat.name] = (
                    feat.name,
                    feature_view_obj.query._left_feature_group,
                )
            for join in feature_view_obj.query.joins:
                for feat in join.query.features:
                    if join.prefix:
                        prefix_feature_map[join.prefix + feat.name] = (
                            feat.name,
                            join.query._left_feature_group,
                        )
                    feature_map[feat.name] = feature_map.get(feat.name, []) + [
                        join.query._left_feature_group
                    ]

            for label_name in feature_view_obj.labels:
                if label_name in prefix_feature_map:
                    feature_view_obj._features.append(
                        training_dataset_feature.TrainingDatasetFeature(
                            name=prefix_feature_map[label_name][0],
                            label=True,
                            featuregroup=prefix_feature_map[label_name][1],
                        )
                    )
                elif label_name in feature_map:
                    if len(feature_map[label_name]) > 1:
                        raise FeatureStoreException(
                            FeatureViewEngine.AMBIGUOUS_LABEL_ERROR.format(label_name)
                        )
                    feature_view_obj._features.append(
                        training_dataset_feature.TrainingDatasetFeature(
                            name=label_name,
                            label=True,
                            featuregroup=feature_map[label_name][0],
                        )
                    )
                else:
                    raise FeatureStoreException(
                        FeatureViewEngine.LABEL_NOT_EXIST_ERROR.format(label_name)
                    )
        self._transformation_function_engine.attach_transformation_fn(feature_view_obj)
        updated_fv = self._feature_view_api.post(feature_view_obj)
        print(
            "Feature view created successfully, explore it at \n"
            + self._get_feature_view_url(updated_fv)
        )
        return updated_fv

    def update(self, feature_view_obj):
        self._feature_view_api.update(feature_view_obj)
        return feature_view_obj

    def get(self, name, version=None):
        if version:
            fv = self._feature_view_api.get_by_name_version(name, version)
            fv.transformation_functions = self.get_attached_transformation_fn(
                fv.name, fv.version
            )
        else:
            fv = self._feature_view_api.get_by_name(name)
            for _fv in fv:
                _fv.transformation_functions = self.get_attached_transformation_fn(
                    _fv.name, _fv.version
                )
        return fv

    def delete(self, name, version=None):
        if version:
            return self._feature_view_api.delete_by_name_version(name, version)
        else:
            return self._feature_view_api.delete_by_name(name)

    def get_batch_query(
        self,
        feature_view_obj,
        start_time,
        end_time,
        with_label=False,
        training_dataset_version=None,
    ):
        try:
            return self._feature_view_api.get_batch_query(
                feature_view_obj.name,
                feature_view_obj.version,
                util.convert_event_time_to_timestamp(start_time),
                util.convert_event_time_to_timestamp(end_time),
                training_dataset_version=training_dataset_version,
                is_python_engine=engine.get_type() == "python",
                with_label=with_label,
            )
        except exceptions.RestAPIError as e:
            if e.response.json().get("errorCode", "") == 270172:
                raise ValueError(
                    "Cannot generate dataset(s) from the given start/end time because"
                    " event time column is not available in the left feature groups."
                    " A start/end time should not be provided as parameters."
                )
            else:
                raise e

    def get_batch_query_string(
        self, feature_view_obj, start_time, end_time, training_dataset_version=None
    ):
        try:
            query_obj = self._feature_view_api.get_batch_query(
                feature_view_obj.name,
                feature_view_obj.version,
                util.convert_event_time_to_timestamp(start_time),
                util.convert_event_time_to_timestamp(end_time),
                training_dataset_version=training_dataset_version,
                is_python_engine=engine.get_type() == "python",
            )
        except exceptions.RestAPIError as e:
            if e.response.json().get("errorCode", "") == 270172:
                raise ValueError(
                    "Cannot generate a query from the given start/end time because"
                    " event time column is not available in the left feature groups."
                    " A start/end time should not be provided as parameters."
                )
            else:
                raise e

        fs_query = self._query_constructor_api.construct_query(query_obj)
        if fs_query.pit_query is not None:
            return fs_query.pit_query
        return fs_query.query

    def get_attached_transformation_fn(self, name, version):
        transformation_functions = (
            self._feature_view_api.get_attached_transformation_fn(name, version)
        )
        if isinstance(transformation_functions, list):
            transformation_functions_dict = dict(
                [
                    (tf.name, tf.transformation_function)
                    for tf in transformation_functions
                ]
            )
        else:
            transformation_functions_dict = {
                transformation_functions.name: transformation_functions.transformation_function
            }
        return transformation_functions_dict

    def create_training_dataset(
        self, feature_view_obj, training_dataset_obj, user_write_options
    ):
        self._set_event_time(feature_view_obj, training_dataset_obj)
        updated_instance = self._create_training_data_metadata(
            feature_view_obj, training_dataset_obj
        )
        td_job = self.compute_training_dataset(
            feature_view_obj,
            user_write_options,
            training_dataset_obj=training_dataset_obj,
        )
        return updated_instance, td_job

    def get_training_data(
        self,
        feature_view_obj,
        read_options=None,
        splits=[],
        training_dataset_obj=None,
        training_dataset_version=None,
    ):
        # check if provided td version has already existed.
        if training_dataset_version:
            td_updated = self._get_training_data_metadata(
                feature_view_obj, training_dataset_version
            )
        else:
            self._set_event_time(feature_view_obj, training_dataset_obj)
            td_updated = self._create_training_data_metadata(
                feature_view_obj, training_dataset_obj
            )
        # check splits
        if len(splits) != len(td_updated.splits):
            if len(td_updated.splits) == 0:
                method_name = "get_training_data"
            elif len(td_updated.splits) == 2:
                method_name = "get_train_test_split"
            elif len(td_updated.splits) == 3:
                method_name = "get_train_validation_test_split"
            raise ValueError(
                f"Incorrect `get` method is used. Use `feature_view.{method_name}` instead."
            )

        read_options = engine.get_instance().read_options(
            td_updated.data_format, read_options
        )

        if td_updated.training_dataset_type != td_updated.IN_MEMORY:
            split_df = self._read_from_storage_connector(
                td_updated, td_updated.splits, read_options, feature_view_obj.schema
            )
        else:
            self._check_feature_group_accessibility(feature_view_obj)
            query = self.get_batch_query(
                feature_view_obj,
                training_dataset_version=td_updated.version,
                start_time=td_updated.event_start_time,
                end_time=td_updated.event_end_time,
                with_label=True,
            )
            split_df = engine.get_instance().get_training_data(
                td_updated, feature_view_obj, query, read_options
            )
            self.compute_training_dataset_statistics(
                feature_view_obj, td_updated, split_df, calc_stat=True
            )

        # split df into features and labels df
        if td_updated.splits:
            for split in td_updated.splits:
                split_name = split.name
                split_df[split_name] = engine.get_instance().split_labels(
                    split_df[split_name], feature_view_obj.labels
                )
            feature_dfs = []
            label_dfs = []
            for split in splits:
                feature_dfs.append(split_df[split][0])
                label_dfs.append(split_df[split][1])
            return td_updated, feature_dfs + label_dfs
        else:
            split_df = engine.get_instance().split_labels(
                split_df, feature_view_obj.labels
            )
            return td_updated, split_df

    def _set_event_time(self, feature_view_obj, training_dataset_obj):
        event_time = feature_view_obj.query._left_feature_group.event_time
        if event_time:
            if training_dataset_obj.splits:
                for split in training_dataset_obj.splits:
                    if (
                        split.split_type == TrainingDatasetSplit.TIME_SERIES_SPLIT
                        and split.name == TrainingDatasetSplit.TRAIN
                        and not split.start_time
                    ):
                        split.start_time = self._get_start_time()
                    if (
                        split.split_type == TrainingDatasetSplit.TIME_SERIES_SPLIT
                        and split.name == TrainingDatasetSplit.TEST
                        and not split.end_time
                    ):
                        split.end_time = self._get_end_time()
            else:
                if not training_dataset_obj.event_start_time:
                    training_dataset_obj.event_start_time = self._get_start_time()
                if not training_dataset_obj.event_end_time:
                    training_dataset_obj.event_end_time = self._get_end_time()

    def _get_start_time(self):
        # minimum start time is 1 second
        return 1000

    def _get_end_time(self):
        # end time is current time
        return int(float(datetime.datetime.now().timestamp()) * 1000)

    def recreate_training_dataset(
        self, feature_view_obj, training_dataset_version, user_write_options
    ):
        training_dataset_obj = self._get_training_data_metadata(
            feature_view_obj, training_dataset_version
        )
        td_job = self.compute_training_dataset(
            feature_view_obj,
            user_write_options,
            training_dataset_obj=training_dataset_obj,
        )
        return training_dataset_obj, td_job

    def _read_from_storage_connector(
        self, training_data_obj, splits, read_options, schema=None
    ):
        if splits:
            result = {}
            for split in splits:
                path = training_data_obj.location + "/" + str(split.name)
                result[split.name] = self._read_dir_from_storage_connector(
                    training_data_obj, path, read_options
                )
            return result
        else:
            path = training_data_obj.location + "/" + training_data_obj.name
            return self._read_dir_from_storage_connector(
                training_data_obj, path, read_options
            )

    def _cast_columns(self, data_format, df, schema):
        if data_format == "csv" or data_format == "tsv":
            if not schema:
                raise FeatureStoreException("Reading csv, tsv requires a schema.")
            return engine.get_instance().cast_columns(df, schema)
        else:
            return df

    def _read_dir_from_storage_connector(self, training_data_obj, path, read_options):
        try:
            return training_data_obj.storage_connector.read(
                # always read from materialized dataset, not query object
                query=None,
                data_format=training_data_obj.data_format,
                options=read_options,
                path=path,
            )
        except Exception as e:
            if isinstance(e, FileNotFoundError):
                raise FileNotFoundError(
                    f"Failed to read dataset from {path}."
                    " Check if path exists or recreate a training dataset."
                )
            else:
                raise e

    # This method is used by hsfs_utils to launch a job for python client
    def compute_training_dataset(
        self,
        feature_view_obj,
        user_write_options,
        training_dataset_obj=None,
        training_dataset_version=None,
    ):
        if training_dataset_obj:
            pass
        elif training_dataset_version:
            training_dataset_obj = self._get_training_data_metadata(
                feature_view_obj, training_dataset_version
            )
        else:
            raise ValueError("No training dataset object or version is provided")

        batch_query = self.get_batch_query(
            feature_view_obj,
            training_dataset_obj.event_start_time,
            training_dataset_obj.event_end_time,
            with_label=True,
            training_dataset_version=training_dataset_obj.version,
        )
        td_job = engine.get_instance().write_training_dataset(
            training_dataset_obj,
            batch_query,
            user_write_options,
            self._OVERWRITE,
            feature_view_obj=feature_view_obj,
        )
        self._td_code_engine.save_code(training_dataset_obj)
        if engine.get_type() == "spark":
            if training_dataset_obj.splits:
                td_df = dict(
                    [
                        (
                            split.name,
                            self._training_dataset_engine.read(
                                training_dataset_obj, split.name, {}
                            ),
                        )
                        for split in training_dataset_obj.splits
                    ]
                )
            else:
                td_df = self._training_dataset_engine.read(
                    training_dataset_obj, None, {}
                )
        else:
            td_df = None
        # currently we do not save the training dataset statistics config for training datasets
        self.compute_training_dataset_statistics(
            feature_view_obj,
            training_dataset_obj,
            td_df,
            calc_stat=engine.get_type() == "spark",
        )
        return td_job

    def compute_training_dataset_statistics(
        self, feature_view_obj, training_dataset_obj, td_df, calc_stat=False
    ):
        if training_dataset_obj.statistics_config.enabled and calc_stat:
            if training_dataset_obj.splits:
                if not isinstance(td_df, dict):
                    raise ValueError(
                        "Provided dataframes should be in dict format "
                        "'split': dataframe"
                    )
                return self._statistics_engine.register_split_statistics(
                    training_dataset_obj,
                    feature_dataframes=td_df,
                    feature_view_obj=feature_view_obj,
                )
            else:
                return self._statistics_engine.compute_statistics(
                    training_dataset_obj,
                    feature_dataframe=td_df,
                    feature_view_obj=feature_view_obj,
                )

    def _get_training_data_metadata(self, feature_view_obj, training_dataset_version):
        td = self._feature_view_api.get_training_dataset_by_version(
            feature_view_obj.name, feature_view_obj.version, training_dataset_version
        )
        # schema and transformation functions need to be set for writing training data or feature serving
        td.schema = feature_view_obj.schema
        td.transformation_functions = feature_view_obj.transformation_functions
        return td

    def _create_training_data_metadata(self, feature_view_obj, training_dataset_obj):
        td = self._feature_view_api.create_training_dataset(
            feature_view_obj.name, feature_view_obj.version, training_dataset_obj
        )
        td.schema = feature_view_obj.schema
        td.transformation_functions = feature_view_obj.transformation_functions
        return td

    def delete_training_data(self, feature_view_obj, training_data_version=None):
        if training_data_version:
            self._feature_view_api.delete_training_data_version(
                feature_view_obj.name, feature_view_obj.version, training_data_version
            )
        else:
            self._feature_view_api.delete_training_data(
                feature_view_obj.name, feature_view_obj.version
            )

    def delete_training_dataset_only(
        self, feature_view_obj, training_data_version=None
    ):
        if training_data_version:
            self._feature_view_api.delete_training_dataset_only_version(
                feature_view_obj.name, feature_view_obj.version, training_data_version
            )
        else:
            self._feature_view_api.delete_training_dataset_only(
                feature_view_obj.name, feature_view_obj.version
            )

    def get_batch_data(
        self,
        feature_view_obj,
        start_time,
        end_time,
        training_dataset_version,
        transformation_functions,
        read_options=None,
    ):
        self._check_feature_group_accessibility(feature_view_obj)

        feature_dataframe = self.get_batch_query(
            feature_view_obj,
            start_time,
            end_time,
            with_label=False,
            training_dataset_version=training_dataset_version,
        ).read(read_options=read_options)
        if transformation_functions:
            return engine.get_instance()._apply_transformation_function(
                transformation_functions, dataset=feature_dataframe
            )
        else:
            return feature_dataframe

    def add_tag(
        self, feature_view_obj, name: str, value, training_dataset_version=None
    ):
        self._tags_api.add(
            feature_view_obj,
            name,
            value,
            training_dataset_version=training_dataset_version,
        )

    def delete_tag(self, feature_view_obj, name: str, training_dataset_version=None):
        self._tags_api.delete(
            feature_view_obj, name, training_dataset_version=training_dataset_version
        )

    def get_tag(self, feature_view_obj, name: str, training_dataset_version=None):
        return self._tags_api.get(
            feature_view_obj, name, training_dataset_version=training_dataset_version
        )[name]

    def get_tags(self, feature_view_obj, training_dataset_version=None):
        return self._tags_api.get(
            feature_view_obj, training_dataset_version=training_dataset_version
        )

    def get_parent_feature_groups(self, feature_view_obj):
        """Get the parents of this feature view, based on explicit provenance.
        Parents are feature groups or external feature groups. These feature
        groups can be accessible, deleted or inaccessible.
        For deleted and inaccessible feature groups, only a minimal information is
        returned.

        # Arguments
            feature_view_obj: Metadata object of feature view.

        # Returns
            `ProvenanceLinks`:  the feature groups used to generated this feature view
        """
        return self._feature_view_api.get_parent_feature_groups(
            feature_view_obj.name, feature_view_obj.version
        )

    def _check_feature_group_accessibility(self, feature_view_obj):
        if (
            engine.get_type() == "python" or engine.get_type() == "hive"
        ) and not feature_view_obj.query.from_cache_feature_group_only():
            raise NotImplementedError(
                "Python kernel can only read from cached feature group."
                " Please use `feature_view.create_training_data` instead."
            )

    def _get_feature_view_url(self, feature_view):
        path = (
            "/p/"
            + str(client.get_instance()._project_id)
            + "/fs/"
            + str(feature_view.featurestore_id)
            + "/fv/"
            + str(feature_view.name)
            + "/version/"
            + str(feature_view.version)
        )
        return util.get_hostname_replaced_url(path)
