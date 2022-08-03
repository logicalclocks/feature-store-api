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
import numpy as np
import avro
import socket
import json

from io import BytesIO
from typing import Dict
from confluent_kafka import Producer
from tqdm.auto import tqdm

from hsfs import client
from hsfs.core import (
    feature_group_api,
    dataset_api,
    job_api,
    ingestion_job_conf,
    kafka_api,
    training_dataset_api,
    training_dataset_job_conf,
    feature_view_api,
)
from hsfs.constructor import query
from hsfs.client import external, hopsworks
from hsfs.feature_group import FeatureGroup
from hsfs.engine import engine_base

HAS_FAST = False
try:
    from fastavro import schemaless_writer
    from fastavro.schema import parse_schema

    HAS_FAST = True
except ImportError:
    pass


class EngineWrite(engine_base.EngineWriteBase):
    def __init__(self):
        self._dataset_api = dataset_api.DatasetApi()
        self._job_api = job_api.JobApi()
        self._kafka_api = kafka_api.KafkaApi()

    def write_training_dataset(
        self,
        training_dataset,
        dataset,
        user_write_options,
        save_mode,
        feature_view_obj=None,
        to_df=False,
    ):
        if not feature_view_obj and not isinstance(dataset, query.Query):
            raise Exception(
                "Currently only query based training datasets are supported by the Python engine"
            )

        # As for creating a feature group, users have the possibility of passing
        # a spark_job_configuration object as part of the user_write_options with the key "spark"
        spark_job_configuration = user_write_options.pop("spark", None)
        td_app_conf = training_dataset_job_conf.TrainingDatsetJobConf(
            query=dataset,
            overwrite=(save_mode == "overwrite"),
            write_options=user_write_options,
            spark_job_configuration=spark_job_configuration,
        )

        if feature_view_obj:
            fv_api = feature_view_api.FeatureViewApi(feature_view_obj.featurestore_id)
            td_job = fv_api.compute_training_dataset(
                feature_view_obj.name,
                feature_view_obj.version,
                training_dataset.version,
                td_app_conf,
            )
        else:
            td_api = training_dataset_api.TrainingDatasetApi(
                training_dataset.feature_store_id
            )
            td_job = td_api.compute(training_dataset, td_app_conf)
        print(
            "Training dataset job started successfully, you can follow the progress at \n{}".format(
                self._get_job_url(td_job.href)
            )
        )

        # If the user passed the wait_for_job option consider it,
        # otherwise use the default True
        self._wait_for_job(td_job, user_write_options)

        return td_job

    def add_file(self, file):
        # if streaming connectors are implemented in the future, this method
        # can be used to materialize certificates locally
        return file

    def register_external_temporary_table(self, external_fg, alias):
        # No op to avoid query failure
        pass

    def register_hudi_temporary_table(
        self, hudi_fg_alias, feature_store_id, feature_store_name, read_options
    ):
        # No op to avoid query failure
        pass

    def save_dataframe(
        self,
        feature_group: FeatureGroup,
        dataframe: pd.DataFrame,
        operation: str,
        online_enabled: bool,
        storage: bool,
        offline_write_options: dict,
        online_write_options: dict,
        validation_id: int = None,
    ):
        if feature_group.stream:
            return self._write_dataframe_kafka(
                feature_group, dataframe, offline_write_options
            )
        else:
            # for backwards compatibility
            return self._legacy_save_dataframe(
                feature_group,
                dataframe,
                operation,
                online_enabled,
                storage,
                offline_write_options,
                online_write_options,
                validation_id,
            )

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
    ):
        raise NotImplementedError(
            "Stream ingestion is not available on Python environments, because it requires Spark as engine."
        )

    def save_empty_dataframe(self, feature_group, dataframe):
        """Wrapper around save_dataframe in order to provide no-op."""
        pass

    # todo only here
    def _legacy_save_dataframe(
        self,
        feature_group,
        dataframe,
        operation,
        online_enabled,
        storage,
        offline_write_options,
        online_write_options,
        validation_id=None,
    ):
        # App configuration
        app_options = self._get_app_options(offline_write_options)

        # Setup job for ingestion
        # Configure Hopsworks ingestion job
        print("Configuring ingestion job...")
        fg_api = feature_group_api.FeatureGroupApi(feature_group.feature_store_id)
        ingestion_job = fg_api.ingestion(feature_group, app_options)

        # Upload dataframe into Hopsworks
        print("Uploading Pandas dataframe...")
        self._dataset_api.upload(feature_group, ingestion_job.data_path, dataframe)

        # Launch job
        print("Launching ingestion job...")
        self._job_api.launch(ingestion_job.job.name)
        print(
            "Ingestion Job started successfully, you can follow the progress at \n{}".format(
                self._get_job_url(ingestion_job.job.href)
            )
        )

        self._wait_for_job(ingestion_job.job, offline_write_options)

        return ingestion_job.job

    # todo only here
    def _get_app_options(self, user_write_options={}):
        """
        Generate the options that should be passed to the application doing the ingestion.
        Options should be data format, data options to read the input dataframe and
        insert options to be passed to the insert method

        Users can pass Spark configurations to the save/insert method
        Property name should match the value in the JobConfiguration.__init__
        """
        spark_job_configuration = user_write_options.pop("spark", None)

        return ingestion_job_conf.IngestionJobConf(
            data_format="PARQUET",
            data_options=[],
            write_options=user_write_options,
            spark_job_configuration=spark_job_configuration,
        )

    # todo only here
    def _write_dataframe_kafka(
        self,
        feature_group: FeatureGroup,
        dataframe: pd.DataFrame,
        offline_write_options: dict,
    ):
        # setup kafka producer
        producer = Producer(self._get_kafka_config(offline_write_options))

        # setup complex feature writers
        feature_writers = {
            feature: self._get_encoder_func(
                feature_group._get_feature_avro_schema(feature)
            )
            for feature in feature_group.get_complex_features()
        }

        # setup row writer function
        writer = self._get_encoder_func(feature_group._get_encoded_avro_schema())

        def acked(err, msg):
            if err is not None and offline_write_options.get("debug_kafka", False):
                print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
            else:
                # update progress bar for each msg
                progress_bar.update()

        # initialize progress bar
        progress_bar = tqdm(
            total=dataframe.shape[0],
            bar_format="{desc}: {percentage:.2f}% |{bar}| Rows {n_fmt}/{total_fmt} | "
            "Elapsed Time: {elapsed} | Remaining Time: {remaining}",
            desc="Uploading Dataframe",
            mininterval=1,
        )
        # loop over rows
        for r in dataframe.itertuples(index=False):
            # itertuples returns Python NamedTyple, to be able to serialize it using
            # avro, create copy of row only by converting to dict, which preserves datatypes
            row = r._asdict()

            # transform special data types
            # here we might need to handle also timestamps and other complex types
            # possible optimizaiton: make it based on type so we don't need to loop over
            # all keys in the row
            for k in row.keys():
                # for avro to be able to serialize them, they need to be python data types
                if isinstance(row[k], np.ndarray):
                    row[k] = row[k].tolist()
                if isinstance(row[k], pd.Timestamp):
                    row[k] = row[k].to_pydatetime()

            # encode complex features
            row = self._encode_complex_features(feature_writers, row)

            # encode feature row
            with BytesIO() as outf:
                writer(row, outf)
                encoded_row = outf.getvalue()

            # assemble key
            key = "".join([str(row[pk]) for pk in sorted(feature_group.primary_key)])

            while True:
                # if BufferError is thrown, we can be sure, message hasn't been send so we retry
                try:
                    # produce
                    producer.produce(
                        topic=feature_group._online_topic_name,
                        key=key,
                        value=encoded_row,
                        callback=acked,
                    )

                    # Trigger internal callbacks to empty op queue
                    producer.poll(0)
                    break
                except BufferError as e:
                    if offline_write_options.get("debug_kafka", False):
                        print("Caught: {}".format(e))
                    # backoff for 1 second
                    producer.poll(1)

        # make sure producer blocks and everything is delivered
        producer.flush()
        progress_bar.close()

        # start backfilling job
        job_name = "{fg_name}_{version}_offline_fg_backfill".format(
            fg_name=feature_group.name, version=feature_group.version
        )
        job = self._job_api.get(job_name)

        if offline_write_options is not None and offline_write_options.get(
            "start_offline_backfill", True
        ):
            print("Launching offline feature group backfill job...")
            self._job_api.launch(job_name)
            print(
                "Backfill Job started successfully, you can follow the progress at \n{}".format(
                    self._get_job_url(job.href)
                )
            )
            self._wait_for_job(job, offline_write_options)

        return job

    # todo only here
    def _encode_complex_features(
        self, feature_writers: Dict[str, callable], row: dict
    ) -> dict:
        for feature_name, writer in feature_writers.items():
            with BytesIO() as outf:
                writer(row[feature_name], outf)
                row[feature_name] = outf.getvalue()
        return row

    # todo only here
    def _get_encoder_func(self, writer_schema: str) -> callable:
        if HAS_FAST:
            schema = json.loads(writer_schema)
            parsed_schema = parse_schema(schema)
            return lambda record, outf: schemaless_writer(outf, parsed_schema, record)

        parsed_schema = avro.schema.parse(writer_schema)
        writer = avro.io.DatumWriter(parsed_schema)
        return lambda record, outf: writer.write(record, avro.io.BinaryEncoder(outf))

    # todo only here
    def _get_kafka_config(self, write_options: dict = {}) -> dict:
        # producer configuration properties
        # https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html
        config = {
            "security.protocol": "SSL",
            "ssl.ca.location": client.get_instance()._get_ca_chain_path(),
            "ssl.certificate.location": client.get_instance()._get_client_cert_path(),
            "ssl.key.location": client.get_instance()._get_client_key_path(),
            "client.id": socket.gethostname(),
            **write_options.get("kafka_producer_config", {}),
        }

        if isinstance(client.get_instance(), hopsworks.Client) or write_options.get(
            "internal_kafka", False
        ):
            config["bootstrap.servers"] = ",".join(
                [
                    endpoint.replace("INTERNAL://", "")
                    for endpoint in self._kafka_api.get_broker_endpoints(
                        externalListeners=False
                    )
                ]
            )
        elif isinstance(client.get_instance(), external.Client):
            config["bootstrap.servers"] = ",".join(
                [
                    endpoint.replace("EXTERNAL://", "")
                    for endpoint in self._kafka_api.get_broker_endpoints(
                        externalListeners=True
                    )
                ]
            )
        return config
