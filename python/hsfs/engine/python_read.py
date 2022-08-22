#
#   Copyright 2022 Hopsworks AB
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

import pandas as pd
import boto3

from io import BytesIO
from typing import Any

from hsfs import util, training_dataset_split
from hsfs.core import dataset_api, transformation_function_engine
from hsfs.engine import engine_base


class EngineRead(engine_base.EngineReadBase):
    def __init__(self):
        self._dataset_api = dataset_api.DatasetApi()

    def read(self, storage_connector, data_format, read_options, location) -> Any:
        if storage_connector.type == storage_connector.HOPSFS:
            df_list = self._read_hopsfs(location, data_format)
        elif storage_connector.type == storage_connector.S3:
            df_list = self._read_s3(storage_connector, location, data_format)
        else:
            raise NotImplementedError(
                "{} Storage Connectors for training datasets are not supported yet for external environments.".format(
                    storage_connector.type
                )
            )
        return pd.concat(df_list, ignore_index=True)

    def read_options(self, data_format, provided_options) -> dict:
        return {}

    def read_stream(
        self,
        storage_connector,
        message_format,
        schema,
        options,
        include_metadata,
    ) -> None:
        raise NotImplementedError(
            "Streaming Sources are not supported for pure Python Environments."
        )

    def show(self, sql_query, feature_store, n, online_conn) -> Any:
        return self.sql(sql_query, feature_store, online_conn, "default", {}).head(n)

    @staticmethod
    def get_unique_values(feature_dataframe, feature_name) -> Any:
        return feature_dataframe[feature_name].unique()

    def get_training_data(
        self, training_dataset_obj, feature_view_obj, query_obj, read_options
    ) -> Any:
        if training_dataset_obj.splits:
            return self._prepare_transform_split_df(
                query_obj, training_dataset_obj, feature_view_obj, read_options
            )
        else:
            df = query_obj.read(read_options=read_options)
            transformation_function_engine.TransformationFunctionEngine.populate_builtin_transformation_functions(
                training_dataset_obj, feature_view_obj, df
            )
            return self._apply_transformation_function(training_dataset_obj, df)

    def get_empty_appended_dataframe(self, dataframe, new_features) -> None:
        """No-op in python engine, user has to write to feature group manually for schema
        change to take effect."""
        return None

    def _read_hopsfs(self, location, data_format):
        # providing more informative error
        try:
            from pydoop import hdfs
        except ModuleNotFoundError:
            return self._read_hopsfs_rest(location, data_format)

        util.setup_pydoop()
        path_list = hdfs.ls(location, recursive=True)

        df_list = []
        for path in path_list:
            if (
                hdfs.path.isfile(path)
                and not path.endswith("_SUCCESS")
                and hdfs.path.getsize(path) > 0
            ):
                df_list.append(self._read_pandas(data_format, path))
        return df_list

    # This is a version of the read method that uses the Hopsworks REST APIs
    # To read the training dataset content, this to avoid the pydoop dependency
    # requirement and allow users to read Hopsworks training dataset from outside
    def _read_hopsfs_rest(self, location, data_format):
        total_count = 10000
        offset = 0
        df_list = []

        while offset < total_count:
            total_count, inode_list = self._dataset_api.list_files(
                location, offset, 100
            )

            for inode in inode_list:
                if not inode.path.endswith("_SUCCESS"):
                    content_stream = self._dataset_api.read_content(inode.path)
                    df_list.append(
                        self._read_pandas(data_format, BytesIO(content_stream.content))
                    )
                offset += 1

        return df_list

    def _read_pandas(self, data_format, obj):
        if data_format.lower() == "csv":
            return pd.read_csv(obj)
        elif data_format.lower() == "tsv":
            return pd.read_csv(obj, sep="\t")
        elif data_format.lower() == "parquet":
            return pd.read_parquet(BytesIO(obj.read()))
        else:
            raise TypeError(
                "{} training dataset format is not supported to read as pandas dataframe.".format(
                    data_format
                )
            )

    def _read_s3(self, storage_connector, location, data_format):
        # get key prefix
        path_parts = location.replace("s3://", "").split("/")
        _ = path_parts.pop(0)  # pop first element -> bucket

        prefix = "/".join(path_parts)

        if storage_connector.session_token is not None:
            s3 = boto3.client(
                "s3",
                aws_access_key_id=storage_connector.access_key,
                aws_secret_access_key=storage_connector.secret_key,
                aws_session_token=storage_connector.session_token,
            )
        else:
            s3 = boto3.client(
                "s3",
                aws_access_key_id=storage_connector.access_key,
                aws_secret_access_key=storage_connector.secret_key,
            )

        df_list = []
        object_list = {"is_truncated": True}
        while object_list.get("is_truncated", False):
            if "NextContinuationToken" in object_list:
                object_list = s3.list_objects_v2(
                    Bucket=storage_connector.bucket,
                    Prefix=prefix,
                    MaxKeys=1000,
                    ContinuationToken=object_list["NextContinuationToken"],
                )
            else:
                object_list = s3.list_objects_v2(
                    Bucket=storage_connector.bucket,
                    Prefix=prefix,
                    MaxKeys=1000,
                )

            for obj in object_list["Contents"]:
                if not obj["Key"].endswith("_SUCCESS") and obj["Size"] > 0:
                    obj = s3.get_object(
                        Bucket=storage_connector.bucket,
                        Key=obj["Key"],
                    )
                    df_list.append(self._read_pandas(data_format, obj["Body"]))
        return df_list

    def _prepare_transform_split_df(
        self, query_obj, training_dataset_obj, feature_view_obj, read_option
    ):
        """
        Split a df into slices defined by `splits`. `splits` is a `dict(str, int)` which keys are name of split
        and values are split ratios.
        """
        if (
            training_dataset_obj.splits[0].split_type
            == training_dataset_split.TrainingDatasetSplit.TIME_SERIES_SPLIT
        ):
            event_time = query_obj._left_feature_group.event_time
            if event_time not in [_feature.name for _feature in query_obj.features]:
                query_obj.append_feature(
                    query_obj._left_feature_group.__getattr__(event_time)
                )
                result_dfs = self._time_series_split(
                    query_obj.read(read_options=read_option),
                    training_dataset_obj,
                    event_time,
                    drop_event_time=True,
                )
            else:
                result_dfs = self._time_series_split(
                    query_obj.read(read_options=read_option),
                    training_dataset_obj,
                    event_time,
                )
        else:
            result_dfs = self._random_split(
                query_obj.read(read_options=read_option), training_dataset_obj
            )

        # apply transformations
        # 1st parametrise transformation functions with dt split stats
        transformation_function_engine.TransformationFunctionEngine.populate_builtin_transformation_functions(
            training_dataset_obj, feature_view_obj, result_dfs
        )
        # and the apply them
        for split_name in result_dfs:
            result_dfs[split_name] = self._apply_transformation_function(
                training_dataset_obj,
                result_dfs.get(split_name),
            )

        return result_dfs

    @staticmethod
    def _apply_transformation_function(training_dataset_instance, dataset):
        for (
            feature_name,
            transformation_fn,
        ) in training_dataset_instance.transformation_functions.items():
            dataset[feature_name] = dataset[feature_name].map(
                transformation_fn.transformation_fn
            )

        return dataset
