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

import os
import json
import importlib.util
from typing import Any

# in case importing in %%local
try:
    from pyspark import SparkFiles
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import struct, concat, col
    from pyspark.sql.avro.functions import to_avro
except ImportError:
    pass

from hsfs import client, util
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core import hudi_engine, transformation_function_engine
from hsfs.constructor import query
from hsfs.engine import engine_base


class EngineWrite(engine_base.EngineWriteBase):
    HIVE_FORMAT = "hive"
    KAFKA_FORMAT = "kafka"

    APPEND = "append"

    def __init__(self):
        self._spark_session = SparkSession.builder.enableHiveSupport().getOrCreate()
        self._spark_context = self._spark_session.sparkContext
        self._jvm = self._spark_context._jvm

        self._spark_session.conf.set("hive.exec.dynamic.partition", "true")
        self._spark_session.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        self._spark_session.conf.set("spark.sql.hive.convertMetastoreParquet", "false")

        if importlib.util.find_spec("pydoop"):
            # If we are on Databricks don't setup Pydoop as it's not available and cannot be easily installed.
            util.setup_pydoop()

    def write_training_dataset(
            self,
            training_dataset,
            query_obj,
            user_write_options,
            save_mode,
            read_options={},
            feature_view_obj=None,
            to_df=False,
    ) -> Any:
        write_options = self._write_options(
            training_dataset.data_format, user_write_options
        )

        if len(training_dataset.splits) == 0:
            if isinstance(query_obj, query.Query):
                dataset = self.convert_to_default_dataframe(
                    query_obj.read(read_options=read_options)
                )
            else:
                raise ValueError("Dataset should be a query.")

            transformation_function_engine.TransformationFunctionEngine.populate_builtin_transformation_functions(
                training_dataset, feature_view_obj, dataset
            )
            if training_dataset.coalesce:
                dataset = dataset.coalesce(1)
            path = training_dataset.location + "/" + training_dataset.name
            return self._write_training_dataset_single(
                training_dataset,
                dataset,
                training_dataset.storage_connector,
                training_dataset.data_format,
                write_options,
                save_mode,
                path,
                to_df=to_df,
            )
        else:
            split_dataset = self._split_df(
                query_obj, training_dataset, read_options=read_options
            )
            transformation_function_engine.TransformationFunctionEngine.populate_builtin_transformation_functions(
                training_dataset, feature_view_obj, split_dataset
            )
            if training_dataset.coalesce:
                for key in split_dataset:
                    split_dataset[key] = split_dataset[key].coalesce(1)
            return self._write_training_dataset_splits(
                training_dataset, split_dataset, write_options, save_mode, to_df=to_df
            )

    def add_file(self, file) -> Any:
        self._spark_context.addFile("hdfs://" + file)
        return SparkFiles.get(os.path.basename(file))

    def register_external_temporary_table(self, external_fg, alias) -> Any:
        external_dataset = external_fg.storage_connector.read(
            external_fg.query,
            external_fg.data_format,
            external_fg.options,
            external_fg.storage_connector._get_path(external_fg.path),
        )
        if external_fg.location:
            self._spark_session.sparkContext.textFile(external_fg.location).collect()

        external_dataset.createOrReplaceTempView(alias)
        return external_dataset

    def register_hudi_temporary_table(
            self, hudi_fg_alias, feature_store_id, feature_store_name, read_options
    ) -> None:
        hudi_engine_instance = hudi_engine.HudiEngine(
            feature_store_id,
            feature_store_name,
            hudi_fg_alias.feature_group,
            self._spark_context,
            self._spark_session,
        )
        hudi_engine_instance.register_temporary_table(
            hudi_fg_alias,
            read_options,
        )

    def save_dataframe(
            self,
            feature_group,
            dataframe,
            operation,
            online_enabled,
            storage,
            offline_write_options,
            online_write_options,
            validation_id=None,
    ) -> None:
        try:
            if feature_group.stream:
                self._save_online_dataframe(
                    feature_group, dataframe, online_write_options
                )
            else:
                if storage == "offline" or not online_enabled:
                    self._save_offline_dataframe(
                        feature_group,
                        dataframe,
                        operation,
                        offline_write_options,
                        validation_id,
                    )
                elif storage == "online":
                    self._save_online_dataframe(
                        feature_group, dataframe, online_write_options
                    )
                elif online_enabled and storage is None:
                    self._save_offline_dataframe(
                        feature_group,
                        dataframe,
                        operation,
                        offline_write_options,
                    )
                    self._save_online_dataframe(
                        feature_group, dataframe, online_write_options
                    )
        except Exception as e:
            raise FeatureStoreException(e)

    def save_stream_dataframe(
            self,
            feature_group,
            dataframe,
            query_name,
            output_mode,
            await_termination,
            timeout,
            checkpoint_dir,
            write_options,
    ) -> Any:
        serialized_df = self._online_fg_to_avro(
            feature_group, self._encode_complex_features(feature_group, dataframe)
        )

        if query_name is None:
            query_name = "insert_stream_" + feature_group._online_topic_name

        query = (
            serialized_df.writeStream.outputMode(output_mode)
            .format(self.KAFKA_FORMAT)
            .option(
                "checkpointLocation",
                "/Projects/"
                + client.get_instance()._project_name
                + "/Resources/"
                + query_name
                + "-checkpoint"
                if checkpoint_dir is None
                else checkpoint_dir,
            )
            .options(**write_options)
            .option("topic", feature_group._online_topic_name)
            .queryName(query_name)
            .start()
        )

        if await_termination:
            query.awaitTermination(timeout)

        return query

    def save_empty_dataframe(self, feature_group, dataframe) -> None:
        """Wrapper around save_dataframe in order to provide no-op in python engine."""
        self.save_dataframe(
            feature_group,
            dataframe,
            "upsert",
            feature_group.online_enabled,
            "offline",
            {},
            {},
        )

    def profile_by_spark(self, metadata_instance) -> None:
        raise NotImplementedError(
            "Profile by spark is only available with Python Engine."
        )

    def _save_offline_dataframe(
            self,
            feature_group,
            dataframe,
            operation,
            write_options,
            validation_id=None,
    ):
        if feature_group.time_travel_format == "HUDI":
            hudi_engine_instance = hudi_engine.HudiEngine(
                feature_group.feature_store_id,
                feature_group.feature_store_name,
                feature_group,
                self._spark_session,
                self._spark_context,
            )

            hudi_engine_instance.save_hudi_fg(
                dataframe, self.APPEND, operation, write_options, validation_id
            )
        else:
            dataframe.write.format(self.HIVE_FORMAT).mode(self.APPEND).options(
                **write_options
            ).partitionBy(
                feature_group.partition_key if feature_group.partition_key else []
            ).saveAsTable(
                feature_group._get_table_name()
            )

    def _save_online_dataframe(self, feature_group, dataframe, write_options):

        serialized_df = self._online_fg_to_avro(
            feature_group, self._encode_complex_features(feature_group, dataframe)
        )
        serialized_df.write.format(self.KAFKA_FORMAT).options(**write_options).option(
            "topic", feature_group._online_topic_name
        ).save()

    def _encode_complex_features(self, feature_group, dataframe):
        """Encodes all complex type features to binary using their avro type as schema."""
        return dataframe.select(
            [
                field["name"]
                if field["name"] not in feature_group.get_complex_features()
                else to_avro(
                    field["name"], feature_group._get_feature_avro_schema(field["name"])
                ).alias(field["name"])
                for field in json.loads(feature_group.avro_schema)["fields"]
            ]
        )

    def _online_fg_to_avro(self, feature_group, dataframe):
        """Packs all features into named struct to be serialized to single avro/binary
        column. And packs primary key into arry to be serialized for partitioning.
        """
        return dataframe.select(
            [
                # be aware: primary_key array should always be sorted
                to_avro(
                    concat(
                        *[
                            col(f).cast("string")
                            for f in sorted(feature_group.primary_key)
                        ]
                    )
                ).alias("key"),
                to_avro(
                    struct(
                        [
                            field["name"]
                            for field in json.loads(feature_group.avro_schema)["fields"]
                        ]
                    ),
                    feature_group._get_encoded_avro_schema(),
                ).alias("value"),
            ]
        )

    def _write_training_dataset_splits(
            self,
            training_dataset,
            feature_dataframes,
            write_options,
            save_mode,
            to_df=False,
    ):
        for split_name, feature_dataframe in feature_dataframes.items():
            split_path = training_dataset.location + "/" + str(split_name)
            feature_dataframes[split_name] = self._write_training_dataset_single(
                training_dataset,
                feature_dataframes[split_name],
                training_dataset.storage_connector,
                training_dataset.data_format,
                write_options,
                save_mode,
                split_path,
                to_df=to_df,
            )

        if to_df:
            return feature_dataframes

    def _write_training_dataset_single(
            self,
            training_dataset,
            feature_dataframe,
            storage_connector,
            data_format,
            write_options,
            save_mode,
            path,
            to_df=False,
    ):
        # apply transformation functions (they are applied separately to each split)
        feature_dataframe = self._apply_transformation_function(
            training_dataset, dataset=feature_dataframe
        )
        if to_df:
            return feature_dataframe
        # TODO: currently not supported petastorm, hdf5 and npy file formats
        if data_format.lower() == "tsv":
            data_format = "csv"

        path = self.setup_storage_connector(storage_connector, path)

        feature_dataframe.write.format(data_format).options(**write_options).mode(
            save_mode
        ).save(path)

    def _write_options(self, data_format, provided_options):
        if data_format.lower() == "tfrecords":
            options = dict(recordType="Example")
            options.update(provided_options)
        elif data_format.lower() == "tfrecord":
            options = dict(recordType="Example")
            options.update(provided_options)
        elif data_format.lower() == "csv":
            options = dict(delimiter=",", header="true")
            options.update(provided_options)
        elif data_format.lower() == "tsv":
            options = dict(delimiter="\t", header="true")
            options.update(provided_options)
        else:
            options = {}
            options.update(provided_options)
        return options

    def _apply_transformation_function(self, training_dataset, dataset):
        # generate transformation function expressions
        transformed_feature_names = []
        transformation_fn_expressions = []
        for (
                feature_name,
                transformation_fn,
        ) in training_dataset.transformation_functions.items():
            fn_registration_name = (
                    transformation_fn.name
                    + "_"
                    + str(transformation_fn.version)
                    + "_"
                    + feature_name
            )
            self._spark_session.udf.register(
                fn_registration_name, transformation_fn.transformation_fn
            )
            transformation_fn_expressions.append(
                "{fn_name:}({name:}) AS {name:}".format(
                    fn_name=fn_registration_name, name=feature_name
                )
            )
            transformed_feature_names.append(feature_name)

        # generate non transformation expressions
        no_transformation_expr = [
            "{name:} AS {name:}".format(name=col_name)
            for col_name in dataset.columns
            if col_name not in transformed_feature_names
        ]

        # generate entire expression and execute it
        transformation_fn_expressions.extend(no_transformation_expr)
        dataset = dataset.selectExpr(*transformation_fn_expressions)

        # sort feature order if it was altered by transformation functions
        sorded_features = sorted(training_dataset._features, key=lambda ft: ft.index)
        sorted_feature_names = [ft.name for ft in sorded_features]
        dataset = dataset.select(*sorted_feature_names)
        return dataset