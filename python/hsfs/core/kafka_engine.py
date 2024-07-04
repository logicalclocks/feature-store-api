#
#   Copyright 2021 Logical Clocks AB
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

import json
from io import BytesIO
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Tuple, Union

from hsfs import client
from hsfs.client import hopsworks
from hsfs.core import storage_connector_api
from hsfs.core.constants import HAS_AVRO, HAS_CONFLUENT_KAFKA, HAS_FAST_AVRO


if HAS_CONFLUENT_KAFKA:
    from confluent_kafka import Consumer, Producer, TopicPartition

if HAS_FAST_AVRO:
    from fastavro import schemaless_writer
    from fastavro.schema import parse_schema
elif HAS_AVRO:
    import avro.io
    import avro.schema


if TYPE_CHECKING:
    from hsfs.feature_group import ExternalFeatureGroup, FeatureGroup


def init_kafka_consumer(
    feature_store_id: int,
    offline_write_options: Dict[str, Any],
) -> Consumer:
    # setup kafka consumer
    consumer_config = get_kafka_config(feature_store_id, offline_write_options)
    if "group.id" not in consumer_config:
        consumer_config["group.id"] = "hsfs_consumer_group"

    return Consumer(consumer_config)


def init_kafka_resources(
    feature_group: Union[FeatureGroup, ExternalFeatureGroup],
    offline_write_options: Dict[str, Any],
) -> Tuple[Producer, Dict[str, Callable], Callable]:
    # setup kafka producer
    producer = init_kafka_producer(
        feature_group.feature_store_id, offline_write_options
    )

    # setup complex feature writers
    feature_writers = {
        feature: get_encoder_func(feature_group._get_feature_avro_schema(feature))
        for feature in feature_group.get_complex_features()
    }

    # setup row writer function
    writer = get_encoder_func(feature_group._get_encoded_avro_schema())
    return producer, feature_writers, writer


def init_kafka_producer(
    feature_store_id: int,
    offline_write_options: Dict[str, Any],
) -> Producer:
    # setup kafka producer
    return Producer(get_kafka_config(feature_store_id, offline_write_options))


def kafka_get_offsets(
    feature_group: Union[FeatureGroup, ExternalFeatureGroup],
    offline_write_options: Dict[str, Any],
    high: bool,
) -> str:
    topic_name = feature_group._online_topic_name
    consumer = init_kafka_consumer(feature_group, offline_write_options)
    topics = consumer.list_topics(
        timeout=offline_write_options.get("kafka_timeout", 6)
    ).topics
    if topic_name in topics.keys():
        # topic exists
        offsets = ""
        tuple_value = int(high)
        for partition_metadata in topics.get(topic_name).partitions.values():
            partition = TopicPartition(
                topic=topic_name, partition=partition_metadata.id
            )
            offsets += f",{partition_metadata.id}:{consumer.get_watermark_offsets(partition)[tuple_value]}"
        consumer.close()

        return f" -initialCheckPointString {topic_name + offsets}"
    return ""


def _kafka_produce(
    producer: Producer,
    feature_group: Union[FeatureGroup, ExternalFeatureGroup],
    key: str,
    encoded_row: bytes,
    acked: callable,
    offline_write_options: Dict[str, Any],
) -> None:
    while True:
        # if BufferError is thrown, we can be sure, message hasn't been send so we retry
        try:
            # produce
            header = {
                "projectId": str(feature_group.feature_store.project_id).encode("utf8"),
                "featureGroupId": str(feature_group._id).encode("utf8"),
                "subjectId": str(feature_group.subject["id"]).encode("utf8"),
            }

            producer.produce(
                topic=feature_group._online_topic_name,
                key=key,
                value=encoded_row,
                callback=acked,
                headers=header,
            )

            # Trigger internal callbacks to empty op queue
            producer.poll(0)
            break
        except BufferError as e:
            if offline_write_options.get("debug_kafka", False):
                print("Caught: {}".format(e))
            # backoff for 1 second
            producer.poll(1)


def encode_complex_features(
    feature_writers: Dict[str, callable], row: Dict[str, Any]
) -> Dict[str, Any]:
    for feature_name, writer in feature_writers.items():
        with BytesIO() as outf:
            writer(row[feature_name], outf)
            row[feature_name] = outf.getvalue()
    return row


def get_encoder_func(writer_schema: str) -> callable:
    if HAS_FAST_AVRO:
        schema = json.loads(writer_schema)
        parsed_schema = parse_schema(schema)
        return lambda record, outf: schemaless_writer(outf, parsed_schema, record)

    parsed_schema = avro.schema.parse(writer_schema)
    writer = avro.io.DatumWriter(parsed_schema)
    return lambda record, outf: writer.write(record, avro.io.BinaryEncoder(outf))


def get_kafka_config(
    feature_store_id: int, write_options: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    if write_options is None:
        write_options = {}
    external = not (
        isinstance(client.get_instance(), hopsworks.Client)
        or write_options.get("internal_kafka", False)
    )

    storage_connector = storage_connector_api.StorageConnectorApi().get_kafka_connector(
        feature_store_id, external
    )

    config = storage_connector.confluent_options()
    config.update(write_options.get("kafka_producer_config", {}))
    return config
