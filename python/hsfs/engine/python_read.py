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
import random
import uuid

from io import BytesIO

from hsfs import util
from hsfs.core import dataset_api, transformation_function_engine
from hsfs.engine import engine_base


class EngineRead(engine_base.EngineReadBase):
    def __init__(self):
        self._dataset_api = dataset_api.DatasetApi()

    def read(self, storage_connector, data_format, read_options, location):
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

    def read_options(self, data_format, provided_options):
        return {}

    def read_stream(
        self,
        storage_connector,
        message_format,
        schema,
        options,
        include_metadata,
    ):
        raise NotImplementedError(
            "Streaming Sources are not supported for pure Python Environments."
        )

    def show(self, sql_query, feature_store, n, online_conn):
        return self.sql(sql_query, feature_store, online_conn, "default", {}).head(n)

    @staticmethod
    def get_unique_values(feature_dataframe, feature_name):
        return feature_dataframe[feature_name].unique()

    def get_training_data(
        self, training_dataset_obj, feature_view_obj, query_obj, read_options
    ):
        df = query_obj.read(read_options=read_options)
        if training_dataset_obj.splits:
            split_df = self._prepare_transform_split_df(
                df, training_dataset_obj, feature_view_obj
            )
        else:
            split_df = df
            transformation_function_engine.TransformationFunctionEngine.populate_builtin_transformation_functions(
                training_dataset_obj, feature_view_obj, split_df
            )
            split_df = self._apply_transformation_function(
                training_dataset_obj, split_df
            )
        return split_df

    def get_empty_appended_dataframe(self, dataframe, new_features):
        """No-op in python engine, user has to write to feature group manually for schema
        change to take effect."""
        return None

    # todo only here
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

    # todo only here
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

    # todo only here
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

    # todo only here
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

    # todo only here
    def _prepare_transform_split_df(self, df, training_dataset_obj, feature_view_obj):
        """
        Split a df into slices defined by `splits`. `splits` is a `dict(str, int)` which keys are name of split
        and values are split ratios.
        """
        split_column = f"_SPLIT_INDEX_{uuid.uuid1()}"
        result_dfs = {}
        splits = training_dataset_obj.splits
        if (
            sum([split.percentage for split in splits]) != 1
            or sum([split.percentage > 1 or split.percentage < 0 for split in splits])
            > 1
        ):
            raise ValueError(
                "Sum of split ratios should be 1 and each values should be in range (0, 1)"
            )

        df_size = len(df)
        groups = []
        for i, split in enumerate(splits):
            groups += [i] * int(df_size * split.percentage)
        groups += [len(splits) - 1] * (df_size - len(groups))
        random.shuffle(groups)
        df[split_column] = groups
        for i, split in enumerate(splits):
            split_df = df[df[split_column] == i].drop(split_column, axis=1)
            result_dfs[split.name] = split_df

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

    # todo only here
    def _apply_transformation_function(self, training_dataset, dataset):
        for (
            feature_name,
            transformation_fn,
        ) in training_dataset.transformation_functions.items():
            dataset[feature_name] = dataset[feature_name].map(
                transformation_fn.transformation_fn
            )

        return dataset
