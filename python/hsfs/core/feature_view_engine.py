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
from __future__ import annotations

import datetime
import warnings
from typing import Dict, List, Optional, Union

from hsfs import (
    client,
    engine,
    feature_group,
    feature_view,
    training_dataset_feature,
    util,
)
from hsfs.client import exceptions
from hsfs.client.exceptions import FeatureStoreException
from hsfs.constructor.filter import Filter, Logic
from hsfs.core import (
    arrow_flight_client,
    code_engine,
    feature_view_api,
    query_constructor_api,
    statistics_engine,
    tags_api,
    training_dataset_engine,
)
from hsfs.core.feature_logging import FeatureLogging
from hsfs.training_dataset_split import TrainingDatasetSplit


class FeatureViewEngine:
    ENTITY_TYPE = "featureview"
    _TRAINING_DATA_API_PATH = "trainingdatasets"
    _OVERWRITE = "overwrite"
    _APPEND = "append"

    _LOG_TD_VERSION = "td_version"
    _LOG_TIME = "log_time"
    _HSML_MODEL = "hsml_model"

    def __init__(self, feature_store_id):
        self._feature_store_id = feature_store_id

        self._feature_view_api = feature_view_api.FeatureViewApi(feature_store_id)
        self._tags_api = tags_api.TagsApi(feature_store_id, self.ENTITY_TYPE)
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

    def save(self, feature_view_obj: FeatureView) -> FeatureView:
        """
        Save a feature view to the backend.

        # Arguments
            feature_view_obj `FeatureView` : The feature view object to be saved.

        # Returns
            `FeatureView` : Updated feature view that has the ID used to save in the backend.
        """
        if feature_view_obj.query.is_time_travel():
            warnings.warn(
                "`as_of` argument in the `Query` will be ignored because"
                " feature view does not support time travel query.",
                stacklevel=1,
            )
        if feature_view_obj.labels:
            for label_name in feature_view_obj.labels:
                (
                    feature,
                    prefix,
                    featuregroup,
                ) = feature_view_obj.query._get_feature_by_name(label_name)
                feature_view_obj._features.append(
                    training_dataset_feature.TrainingDatasetFeature(
                        name=feature.name,
                        label=True,
                        featuregroup=featuregroup,
                    )
                )
        if feature_view_obj.inference_helper_columns:
            for helper_column_name in feature_view_obj.inference_helper_columns:
                (
                    feature,
                    prefix,
                    featuregroup,
                ) = feature_view_obj.query._get_feature_by_name(helper_column_name)
                feature_view_obj._features.append(
                    training_dataset_feature.TrainingDatasetFeature(
                        name=feature.name,
                        inference_helper_column=True,
                        featuregroup=featuregroup,
                    )
                )

        if feature_view_obj.training_helper_columns:
            for helper_column_name in feature_view_obj.training_helper_columns:
                (
                    feature,
                    prefix,
                    featuregroup,
                ) = feature_view_obj.query._get_feature_by_name(helper_column_name)
                feature_view_obj._features.append(
                    training_dataset_feature.TrainingDatasetFeature(
                        name=feature.name,
                        training_helper_column=True,
                        featuregroup=featuregroup,
                    )
                )

        updated_fv = self._feature_view_api.post(feature_view_obj)
        print(
            "Feature view created successfully, explore it at \n"
            + self._get_feature_view_url(updated_fv)
        )
        return updated_fv

    def update(self, feature_view_obj: FeatureView) -> FeatureView:
        """
        Update the feature view object saved in the backend

        # Arguments
            feature_view_obj `FeatureView` : The feature view object to be saved.

        # Returns
            `FeatureView` : Updated feature view that has the ID used to save in the backend.
        """
        self._feature_view_api.update(feature_view_obj)
        return feature_view_obj

    def get(
        self, name: str, version: int = None
    ) -> Union[FeatureView, List[FeatureView]]:
        """
        Get a feature view form the backend using name or using name and version.

        If version is not provided then a List of feature views containing all of its versions is returned.

        # Arguments
            name `str`: Name of feature view.
            version `version`: Version of the feature view.

        # Returns
            `Union[FeatureView, List[FeatureView]]`

        # Raises
            `RestAPIError`: If the feature view cannot be found from the backend.
            `ValueError`: If the feature group associated with the feature view cannot be found.
        """
        if version:
            fv = self._feature_view_api.get_by_name_version(name, version)
        else:
            fv = self._feature_view_api.get_by_name(name)
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
        primary_keys=False,
        event_time=False,
        inference_helper_columns=False,
        training_helper_columns=False,
        training_dataset_version=None,
        spine=None,
    ):
        try:
            query = self._feature_view_api.get_batch_query(
                feature_view_obj.name,
                feature_view_obj.version,
                util.convert_event_time_to_timestamp(start_time),
                util.convert_event_time_to_timestamp(end_time),
                training_dataset_version=training_dataset_version,
                is_python_engine=engine.get_type() == "python",
                with_label=with_label,
                primary_keys=primary_keys,
                event_time=event_time,
                inference_helper_columns=inference_helper_columns,
                training_helper_columns=training_helper_columns,
            )
            # verify whatever is passed 1. spine group with dataframe contained, or 2. dataframe
            # the schema has to be consistent

            # allow passing new spine group or dataframe
            if isinstance(spine, feature_group.SpineGroup):
                # schema of original fg on left side needs to be consistent with schema contained in the
                # spine group to overwrite the feature group
                dataframe_features = engine.get_instance().parse_schema_feature_group(
                    spine.dataframe
                )
                spine._feature_group_engine._verify_schema_compatibility(
                    query._left_feature_group.features, dataframe_features
                )
                query._left_feature_group = spine
            elif isinstance(query._left_feature_group, feature_group.SpineGroup):
                if spine is None:
                    raise FeatureStoreException(
                        "Feature View was created with a spine group, setting the `spine` argument is mandatory."
                    )
                # the dataframe setter will verify the schema of the dataframe
                query._left_feature_group.dataframe = spine
            return query
        except exceptions.RestAPIError as e:
            if e.response.json().get("errorCode", "") == 270172:
                raise ValueError(
                    "Cannot generate dataset(s) from the given start/end time because"
                    " event time column is not available in the left feature groups."
                    " A start/end time should not be provided as parameters."
                ) from e
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
                ) from e
            else:
                raise e

        fs_query = self._query_constructor_api.construct_query(query_obj)
        if fs_query.pit_query is not None:
            return fs_query.pit_query
        return fs_query.query

    def get_attached_transformation_fn(
        self, name: str, version: int
    ) -> List[TransformationFunction]:
        """
        Get transformation functions attached to a feature view form the backend

        # Arguments
            name `str`: Name of feature view.
            version `ìnt`: Version of feature view.

        # Returns
            `List[TransformationFunction]` : List of transformation functions attached to the feature view.

        # Raises
            `RestAPIError`: If the feature view cannot be found from the backend.
            `ValueError`: If the feature group associated with the feature view cannot be found.
        """
        transformation_functions = (
            self._feature_view_api.get_attached_transformation_fn(name, version)
        )
        return transformation_functions

    def create_training_dataset(
        self,
        feature_view_obj,
        training_dataset_obj,
        user_write_options,
        spine=None,
        primary_keys=False,
        event_time=False,
        training_helper_columns=False,
    ):
        self._set_event_time(feature_view_obj, training_dataset_obj)
        updated_instance = self._create_training_data_metadata(
            feature_view_obj, training_dataset_obj
        )
        td_job = self.compute_training_dataset(
            feature_view_obj,
            user_write_options,
            training_dataset_obj=training_dataset_obj,
            spine=spine,
            primary_keys=primary_keys,
            event_time=event_time,
            training_helper_columns=training_helper_columns,
        )
        return updated_instance, td_job

    def get_training_data(
        self,
        feature_view_obj,
        read_options=None,
        splits=None,
        training_dataset_obj=None,
        training_dataset_version=None,
        spine=None,
        primary_keys=False,
        event_time=False,
        training_helper_columns=False,
        dataframe_type="default",
    ):
        # check if provided td version has already existed.
        if training_dataset_version:
            td_updated = self._get_training_dataset_metadata(
                feature_view_obj, training_dataset_version
            )
        else:
            self._set_event_time(feature_view_obj, training_dataset_obj)
            td_updated = self._create_training_data_metadata(
                feature_view_obj, training_dataset_obj
            )
        # check splits
        if splits is None:
            splits = []
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
                td_updated,
                td_updated.splits,
                read_options,
                with_primary_keys=primary_keys,
                # at this stage training dataset was already written and if there was any name clash it should have
                # already failed in creation phase, so we don't need to check it here. This is to make
                # sure that both it is backwards compatible and also to drop columns if user set
                # primary_keys and event_times to True on creation stage.
                primary_keys=self._get_primary_keys_from_query(
                    feature_view_obj.query, False
                ),
                with_event_time=event_time,
                event_time=self._get_eventtimes_from_query(
                    feature_view_obj.query, False
                ),
                with_training_helper_columns=training_helper_columns,
                training_helper_columns=feature_view_obj.training_helper_columns,
                feature_view_features=[
                    feature.name for feature in feature_view_obj.features
                ],
                # forcing dataframe type to default here since dataframe operations are required for training data split.
                dataframe_type="default"
                if dataframe_type.lower() in ["numpy", "python"]
                else dataframe_type,  # forcing dataframe type to default here since dataframe operations are required for training data split.
            )
        else:
            self._check_feature_group_accessibility(feature_view_obj)
            query = self.get_batch_query(
                feature_view_obj,
                training_dataset_version=td_updated.version,
                start_time=td_updated.event_start_time,
                end_time=td_updated.event_end_time,
                with_label=True,
                inference_helper_columns=False,
                primary_keys=primary_keys,
                event_time=event_time,
                training_helper_columns=training_helper_columns,
                spine=spine,
            )
            split_df = engine.get_instance().get_training_data(
                td_updated,
                feature_view_obj,
                query,
                read_options,
                dataframe_type,
                training_dataset_version,
            )
            self.compute_training_dataset_statistics(
                feature_view_obj, td_updated, split_df
            )

        # split df into features and labels df
        if td_updated.splits:
            for split in td_updated.splits:
                split_name = split.name
                split_df[split_name] = engine.get_instance().split_labels(
                    split_df[split_name], feature_view_obj.labels, dataframe_type
                )
            feature_dfs = []
            label_dfs = []
            for split in splits:
                feature_dfs.append(split_df[split][0])
                label_dfs.append(split_df[split][1])
            return td_updated, feature_dfs + label_dfs
        else:
            split_df = engine.get_instance().split_labels(
                split_df, feature_view_obj.labels, dataframe_type
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
        self,
        feature_view_obj,
        training_dataset_version,
        statistics_config,
        user_write_options,
        spine=None,
    ):
        training_dataset_obj = self._get_training_dataset_metadata(
            feature_view_obj, training_dataset_version
        )

        if statistics_config is not None:
            # update statistics config if provided. This is currently the only way to update TD statistics config.
            # recreating a training dataset may result in different statistics if the FGs data in the FV query have been update.
            training_dataset_obj.statistics_config = statistics_config
            training_dataset_obj.update_statistics_config()

        td_job = self.compute_training_dataset(
            feature_view_obj,
            user_write_options,
            training_dataset_obj=training_dataset_obj,
            spine=spine,
        )
        return training_dataset_obj, td_job

    def _read_from_storage_connector(
        self,
        training_data_obj,
        splits,
        read_options,
        with_primary_keys,
        primary_keys,
        with_event_time,
        event_time,
        with_training_helper_columns,
        training_helper_columns,
        feature_view_features,
        dataframe_type,
    ):
        if splits:
            result = {}
            for split in splits:
                path = training_data_obj.location + "/" + str(split.name)
                result[split.name] = self._read_dir_from_storage_connector(
                    training_data_obj,
                    path,
                    read_options,
                    with_primary_keys,
                    primary_keys,
                    with_event_time,
                    event_time,
                    with_training_helper_columns,
                    training_helper_columns,
                    feature_view_features,
                    dataframe_type,
                )
            return result
        else:
            path = training_data_obj.location + "/" + training_data_obj.name
            return self._read_dir_from_storage_connector(
                training_data_obj,
                path,
                read_options,
                with_primary_keys,
                primary_keys,
                with_event_time,
                event_time,
                with_training_helper_columns,
                training_helper_columns,
                feature_view_features,
                dataframe_type,
            )

    def _cast_columns(self, data_format, df, schema):
        if data_format == "csv" or data_format == "tsv":
            if not schema:
                raise FeatureStoreException("Reading csv, tsv requires a schema.")
            return engine.get_instance().cast_columns(df, schema)
        else:
            return df

    def _read_dir_from_storage_connector(
        self,
        training_data_obj,
        path,
        read_options,
        with_primary_keys,
        primary_keys,
        with_event_time,
        event_time,
        with_training_helper_columns,
        training_helper_columns,
        feature_view_features,
        dataframe_type,
    ):
        try:
            df = training_data_obj.storage_connector.read(
                # always read from materialized dataset, not query object
                query=None,
                data_format=training_data_obj.data_format,
                options=read_options,
                path=path,
                dataframe_type=dataframe_type,
            )

            df = self._drop_helper_columns(
                df,
                feature_view_features,
                with_primary_keys,
                primary_keys,
                False,
                dataframe_type,
            )
            df = self._drop_helper_columns(
                df,
                feature_view_features,
                with_event_time,
                event_time,
                False,
                dataframe_type,
            )
            df = self._drop_helper_columns(
                df,
                feature_view_features,
                with_training_helper_columns,
                training_helper_columns,
                True,
                dataframe_type,
            )
            return df

        except Exception as e:
            if isinstance(e, FileNotFoundError):
                raise FileNotFoundError(
                    f"Failed to read dataset from {path}."
                    " Check if path exists or recreate a training dataset."
                ) from e
            else:
                raise e

    def _drop_helper_columns(
        self,
        df,
        feature_view_features,
        with_columns,
        columns,
        training_helper,
        dataframe_type,
    ):
        if not with_columns:
            if (
                engine.get_type().startswith("spark")
                and dataframe_type.lower() == "spark"
            ):
                existing_cols = [field.name for field in df.schema.fields]
            else:
                existing_cols = df.columns
            # primary keys and event time are dropped only if they are in the query
            drop_cols = list(set(existing_cols).intersection(columns))
            # training helper is always in the query
            if not training_helper:
                drop_cols = list(set(drop_cols).difference(feature_view_features))
            if drop_cols:
                df = engine.get_instance().drop_columns(df, drop_cols)
        return df

    # This method is used by hsfs_utils to launch a job for python client
    def compute_training_dataset(
        self,
        feature_view_obj,
        user_write_options,
        training_dataset_obj=None,
        training_dataset_version=None,
        spine=None,
        primary_keys=False,
        event_time=False,
        training_helper_columns=False,
    ):
        if training_dataset_obj:
            pass
        elif training_dataset_version:
            training_dataset_obj = self._get_training_dataset_metadata(
                feature_view_obj, training_dataset_version
            )
        else:
            raise ValueError("No training dataset object or version is provided")

        batch_query = self.get_batch_query(
            feature_view_obj,
            training_dataset_obj.event_start_time,
            training_dataset_obj.event_end_time,
            with_label=True,
            primary_keys=primary_keys,
            event_time=event_time,
            inference_helper_columns=False,
            training_helper_columns=training_helper_columns,
            training_dataset_version=training_dataset_obj.version,
            spine=spine,
        )

        # for spark job
        user_write_options["training_helper_columns"] = training_helper_columns
        user_write_options["primary_keys"] = primary_keys
        user_write_options["event_time"] = event_time

        td_job = engine.get_instance().write_training_dataset(
            training_dataset_obj,
            batch_query,
            user_write_options,
            self._OVERWRITE,
            feature_view_obj=feature_view_obj,
        )
        self._td_code_engine.save_code(training_dataset_obj)

        if engine.get_type().startswith("spark"):
            # if spark engine, read td and compute stats
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
            self.compute_training_dataset_statistics(
                feature_view_obj, training_dataset_obj, td_df
            )

        return td_job

    def compute_training_dataset_statistics(
        self, feature_view_obj, training_dataset_obj, td_df
    ):
        if training_dataset_obj.statistics_config.enabled:
            if training_dataset_obj.splits:
                if not isinstance(td_df, dict):
                    raise ValueError(
                        "Provided dataframes should be in dict format "
                        "'split': dataframe"
                    )
                return self._statistics_engine.compute_and_save_split_statistics(
                    training_dataset_obj,
                    feature_dataframes=td_df,
                    feature_view_obj=feature_view_obj,
                )
            else:
                return self._statistics_engine.compute_and_save_statistics(
                    training_dataset_obj,
                    feature_dataframe=td_df,
                    feature_view_obj=feature_view_obj,
                )

    def _get_training_dataset_metadata(
        self, feature_view_obj, training_dataset_version
    ):
        td = self._feature_view_api.get_training_dataset_by_version(
            feature_view_obj.name, feature_view_obj.version, training_dataset_version
        )
        # schema needs to be set for writing training data or feature serving
        td.schema = feature_view_obj.schema
        return td

    def _get_training_datasets_metadata(self, feature_view_obj):
        tds = self._feature_view_api.get_training_datasets(
            feature_view_obj.name, feature_view_obj.version
        )
        # schema needs to be set for writing training data or feature serving
        for td in tds:
            td.schema = feature_view_obj.schema
        return tds

    def get_training_datasets(self, feature_view_obj):
        tds = self._get_training_datasets_metadata(feature_view_obj)
        # this is the only place we expose training dataset metadata
        # we return training dataset base classes with metadata only
        return [super(td.__class__, td) for td in tds]

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
        spine=None,
        primary_keys=False,
        event_time=False,
        inference_helper_columns=False,
        dataframe_type="default",
        transformed=True,
    ):
        self._check_feature_group_accessibility(feature_view_obj)

        # check if primary_keys/event_time are ambiguous
        if primary_keys:
            self._get_primary_keys_from_query(feature_view_obj.query)
        if event_time:
            self._get_eventtimes_from_query(feature_view_obj.query)

        feature_dataframe = self.get_batch_query(
            feature_view_obj,
            start_time,
            end_time,
            with_label=False,
            primary_keys=primary_keys,
            event_time=event_time,
            inference_helper_columns=inference_helper_columns or transformed,
            training_helper_columns=False,
            training_dataset_version=training_dataset_version,
            spine=spine,
        ).read(read_options=read_options, dataframe_type=dataframe_type)
        if transformation_functions and transformed:
            return engine.get_instance()._apply_transformation_function(
                transformation_functions, dataset=feature_dataframe
            )
        else:
            return feature_dataframe

    def transform_batch_data(self, features, transformation_functions):
        return engine.get_instance()._apply_transformation_function(
                transformation_functions, dataset=features, inplace=False
            )

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

    def get_models_provenance(
        self, feature_view_obj, training_dataset_version: Optional[int] = None
    ):
        """Get the generated models using this feature view, based on explicit
        provenance. These models can be accessible or inaccessible. Explicit
        provenance does not track deleted generated model links, so deleted
        will always be empty.
        For inaccessible models, only a minimal information is returned.

        # Arguments
            feature_view_obj: Filter generated models based on feature view (name, version).
            training_dataset_version: Filter generated models based on the used training dataset version.

        # Returns
            `ProvenanceLinks`:  the models generated using this feature group
        """
        return self._feature_view_api.get_models_provenance(
            feature_view_obj.name,
            feature_view_obj.version,
            training_dataset_version=training_dataset_version,
        )

    def _check_feature_group_accessibility(self, feature_view_obj):
        if engine.get_type() in ["python", "hive"]:
            if arrow_flight_client.get_instance().is_enabled():
                if not arrow_flight_client.supports(
                    feature_view_obj.query.featuregroups
                ):
                    raise NotImplementedError(
                        "Hopsworks Feature Query Service can only read from cached feature groups"
                        " and external feature groups on BigQuery and Snowflake."
                        " When using other external feature groups please use "
                        "`feature_view.create_training_data` instead. "
                        "If you are using spines, use a Spark Kernel."
                    )
            elif not feature_view_obj.query.is_cache_feature_group_only():
                raise NotImplementedError(
                    "Python kernel can only read from cached feature groups."
                    " When using external feature groups please use "
                    "`feature_view.create_training_data` instead. "
                    "If you are using spines, use a Spark Kernel."
                )

    def _get_feature_view_url(self, fv: "feature_view.FeatureView"):
        path = (
            "/p/"
            + str(client.get_instance()._project_id)
            + "/fs/"
            + str(fv.featurestore_id)
            + "/fv/"
            + str(fv.name)
            + "/version/"
            + str(fv.version)
        )
        return util.get_hostname_replaced_url(path)

    def _get_primary_keys_from_query(self, fv_query_obj, check_duplicate=True):
        fv_pks = set(
            [
                feature.name
                for feature in fv_query_obj._left_feature_group.features
                if feature.primary
            ]
        )
        for _join in fv_query_obj._joins:
            fv_pks.update(
                [
                    (
                        self._check_if_exists_with_prefix(feature.name, fv_pks)
                        if check_duplicate
                        else feature.name
                    )
                    if _join.prefix is None
                    else _join.prefix + feature.name
                    for feature in _join.query._left_feature_group.features
                    if feature.primary
                ]
            )

        return list(fv_pks)

    def _get_eventtimes_from_query(self, fv_query_obj, check_duplicate=True):
        fv_events = set()
        if fv_query_obj._left_feature_group.event_time:
            fv_events.update([fv_query_obj._left_feature_group.event_time])
        for _join in fv_query_obj._joins:
            if _join.query._left_feature_group.event_time:
                fv_events.update(
                    [
                        (
                            self._check_if_exists_with_prefix(
                                _join.query._left_feature_group.event_time, fv_events
                            )
                            if check_duplicate
                            else _join.query._left_feature_group.event_time
                        )
                        if _join.prefix is None
                        else _join.prefix + _join.query._left_feature_group.event_time
                    ]
                )

        return list(fv_events)

    def _check_if_exists_with_prefix(self, f_name, f_set):
        if f_name in f_set:
            raise FeatureStoreException(
                f"Provided feature {f_name} is ambiguous and exists in more than one feature groups."
                "To avoid this error specify prefix in the join."
            )
        else:
            return f_name

    def enable_feature_logging(self, fv):
        self._feature_view_api.enable_feature_logging(fv.name, fv.version)
        fv.logging_enabled = True
        return fv

    def get_feature_logging(self, fv):
        return FeatureLogging.from_response_json(
            self._feature_view_api.get_feature_logging(fv.name, fv.version)
        )

    def _get_logging_fg(self, fv, transformed):
        feature_logging = self.get_feature_logging(fv)
        if transformed:
            return feature_logging.transformed_features
        else:
            return feature_logging.untransformed_features

    def log_features(self, fv, features, prediction=None, transformed=False, write_options=None, training_dataset_version=None, hsml_model=None):
        default_write_options = {
            "start_offline_materialization": False,
        }
        if write_options:
            default_write_options.update(write_options)
        fg = self._get_logging_fg(fv, transformed)
        df = engine.get_instance().get_feature_logging_df(
            fg,
            features,
            [feature for feature in fv.features if not feature.label],
            [feature for feature in fv.features if feature.label],
            FeatureViewEngine._LOG_TD_VERSION,
            FeatureViewEngine._LOG_TIME,
            FeatureViewEngine._HSML_MODEL,
            prediction,
            training_dataset_version,
            hsml_model,
        )
        return fg.insert(df, write_options=default_write_options)

    def read_feature_logs(self, fv,
                          start_time: Optional[
                     Union[str, int, datetime, datetime.date]] = None,
                          end_time: Optional[
                     Union[str, int, datetime, datetime.date]] = None,
                          filter: Optional[Union[Filter, Logic]]=None,
                          transformed: Optional[bool]=False,
                          training_dataset_version=None,
                          hsml_model=None,
                          ):
        fg = self._get_logging_fg(fv, transformed)
        fv_feat_name_map = self._get_fv_feature_name_map(fv)
        query = fg.select_all()
        if start_time:
            query = query.filter(fg.get_feature(FeatureViewEngine._LOG_TIME) >= start_time)
        if end_time:
            query = query.filter(fg.get_feature(FeatureViewEngine._LOG_TIME) <= end_time)
        if training_dataset_version:
            query = query.filter(fg.get_feature(FeatureViewEngine._LOG_TD_VERSION) == training_dataset_version)
        if hsml_model:
            query = query.filter(fg.get_feature(FeatureViewEngine._HSML_MODEL) == self.get_hsml_model_value(hsml_model))
        if filter:
            query = query.filter(self._convert_to_log_fg_filter(fg, fv, filter, fv_feat_name_map))
        df = query.read()
        df = df.drop(["log_id", FeatureViewEngine._LOG_TIME], axis=1)
        return df

    @staticmethod
    def get_hsml_model_value(hsml_model):
        return f"{hsml_model.name}_{hsml_model.version}"

    def _convert_to_log_fg_filter(self, fg, fv, filter, fv_feat_name_map):
        if filter is None:
            return None

        if isinstance(filter, Logic):
            return Logic(
                filter.type,
                left_f=self._convert_to_log_fg_filter(fv, filter.left_f),
                right_f=self._convert_to_log_fg_filter(fv, filter.right_f),
                left_l=self._convert_to_log_fg_filter(fv, filter.left_l),
                right_l=self._convert_to_log_fg_filter(fv, filter.right_l),
            )
        elif isinstance(filter, Filter):
            fv_feature_name = fv_feat_name_map.get(
                    f"{filter.feature.feature_group_id}_{filter.feature.name}")
            if fv_feature_name is None:
                raise FeatureStoreException("Filter feature {filter.feature.name} does not exist in feature view feature.")
            return Filter(
                fg.get_feature(filter.feature.name),
                filter.condition,
                filter.value,
            )
        else:
            raise FeatureStoreException("Accept only Filter or Logic")

    def _get_fv_feature_name_map(self, fv) -> Dict[str, str]:
        result_dict = {}
        for td_feature in fv.features:
            fg_feature_key = f"{td_feature.feature_group.id}_{td_feature.feature_group_feature_name}"
            result_dict[fg_feature_key] = td_feature.name
        return result_dict

    def get_log_timeline(self, fv,
                         wallclock_time: Optional[
                               Union[str, int, datetime, datetime.date]] = None,
                         limit: Optional[int] = None,
                         transformed: Optional[bool]=False,
                         ) -> Dict[str, Dict[str, str]]:
        fg = self._get_logging_fg(fv, transformed)
        return fg.commit_details(wallclock_time=wallclock_time, limit=limit)

    def pause_logging(self, fv):
        self._feature_view_api.pause_feature_logging(
            fv.name, fv.version
        )
    def resume_logging(self, fv):
        self._feature_view_api.resume_feature_logging(
            fv.name, fv.version
        )

    def materialize_feature_logs(self, fv, wait):
        jobs = self._feature_view_api.materialize_feature_logging(
            fv.name, fv.version
        )
        if wait:
            for job in jobs:
                try:
                    job._wait_for_job(wait)
                except Exception:
                    pass
        return jobs

    def delete_feature_logs(self, fv, transformed):
        self._feature_view_api.delete_feature_logs(
            fv.name, fv.version, transformed
        )
