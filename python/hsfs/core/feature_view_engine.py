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
from hsfs import engine, training_dataset_feature, client, util, feature_group
from hsfs.client import exceptions
from hsfs.client.exceptions import FeatureStoreException
from hsfs.training_dataset_split import TrainingDatasetSplit
from hsfs.core import (
    tags_api,
    transformation_function_engine,
    feature_view_api,
    code_engine,
    statistics_engine,
    training_dataset_engine,
    query_constructor_api,
    arrow_flight_client,
)


class FeatureViewEngine:
    ENTITY_TYPE = "featureview"
    _TRAINING_DATA_API_PATH = "trainingdatasets"
    _OVERWRITE = "overwrite"
    _APPEND = "append"

    def __init__(self, feature_store_id):
        self._feature_store_id = feature_store_id

        self._feature_view_api = feature_view_api.FeatureViewApi(feature_store_id)
        self._tags_api = tags_api.TagsApi(feature_store_id, self.ENTITY_TYPE)
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

        self._transformation_function_engine.attach_transformation_fn(feature_view_obj)
        updated_fv = self._feature_view_api.post(feature_view_obj)
        self.attach_transformation_function(updated_fv)
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
            self.attach_transformation_function(fv)
        else:
            fv = self._feature_view_api.get_by_name(name)
            for _fv in fv:
                self.attach_transformation_function(_fv)
        return fv

    def attach_transformation_function(self, fv):
        fv.transformation_functions = self.get_attached_transformation_fn(
            fv.name, fv.version
        )
        if fv.transformation_functions:
            for feature in fv.schema:
                feature.transformation_function = fv.transformation_functions.get(
                    feature.name, None
                )

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
        splits=[],
        training_dataset_obj=None,
        training_dataset_version=None,
        spine=None,
        primary_keys=False,
        event_time=False,
        training_helper_columns=False,
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
                td_updated,
                td_updated.splits,
                read_options,
                with_primary_keys=primary_keys,
                primary_keys=self._get_primary_keys_from_query(feature_view_obj.query),
                with_event_time=event_time,
                event_time=[feature_view_obj.query._left_feature_group.event_time],
                with_training_helper_columns=training_helper_columns,
                training_helper_columns=feature_view_obj.training_helper_columns,
                feature_view_features=[
                    feature.name for feature in feature_view_obj.features
                ],
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
        self, feature_view_obj, training_dataset_version, user_write_options, spine=None
    ):
        training_dataset_obj = self._get_training_data_metadata(
            feature_view_obj, training_dataset_version
        )
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
    ):
        try:
            df = training_data_obj.storage_connector.read(
                # always read from materialized dataset, not query object
                query=None,
                data_format=training_data_obj.data_format,
                options=read_options,
                path=path,
            )

            df = self._drop_helper_columns(
                df, feature_view_features, with_primary_keys, primary_keys, False
            )
            df = self._drop_helper_columns(
                df, feature_view_features, with_event_time, event_time, False
            )
            df = self._drop_helper_columns(
                df,
                feature_view_features,
                with_training_helper_columns,
                training_helper_columns,
                True,
            )
            return df

        except Exception as e:
            if isinstance(e, FileNotFoundError):
                raise FileNotFoundError(
                    f"Failed to read dataset from {path}."
                    " Check if path exists or recreate a training dataset."
                )
            else:
                raise e

    def _drop_helper_columns(
        self, df, feature_view_features, with_columns, columns, training_helper
    ):
        if not with_columns:
            if engine.get_type() == "spark":
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
        spine=None,
        primary_keys=False,
        event_time=False,
        inference_helper_columns=False,
    ):
        self._check_feature_group_accessibility(feature_view_obj)

        feature_dataframe = self.get_batch_query(
            feature_view_obj,
            start_time,
            end_time,
            with_label=False,
            primary_keys=primary_keys,
            event_time=event_time,
            inference_helper_columns=inference_helper_columns,
            training_helper_columns=False,
            training_dataset_version=training_dataset_version,
            spine=spine,
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
        if engine.get_type() in ["python", "hive"]:
            if arrow_flight_client.get_instance().is_enabled():
                if not arrow_flight_client.get_instance().supports(
                    feature_view_obj.query.featuregroups
                ):
                    raise NotImplementedError(
                        "ArrowFlightServer can only read from cached feature groups"
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

    def _get_primary_keys_from_query(self, fv_query_obj):
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
                    feature.name
                    for feature in _join.query._left_feature_group.features
                    if feature.primary
                ]
            )

        return list(fv_pks)
