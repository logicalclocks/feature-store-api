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

from typing import TYPE_CHECKING, Any, Callable, Dict, Tuple, Union


try:
    from confluent_kafka import Consumer, Producer
except ImportError:
    pass

if TYPE_CHECKING:
    from hsfs import ExternalFeatureGroup, FeatureGroup


class KafkaEngine:
    def _init_kafka_consumer(
        self,
        feature_group: Union[FeatureGroup, ExternalFeatureGroup],
        offline_write_options: Dict[str, Any],
    ) -> Consumer:
        # setup kafka consumer
        consumer_config = self._get_kafka_config(
            feature_group.feature_store_id, offline_write_options
        )
        if "group.id" not in consumer_config:
            consumer_config["group.id"] = "hsfs_consumer_group"

        return Consumer(consumer_config)

    def _init_kafka_resources(
        self,
        feature_group: Union[FeatureGroup, ExternalFeatureGroup],
        offline_write_options: Dict[str, Any],
    ) -> Tuple[Producer, Dict[str, Callable], Callable]:
        # setup kafka producer
        producer = self._init_kafka_producer(feature_group, offline_write_options)

        # setup complex feature writers
        feature_writers = {
            feature: self._get_encoder_func(
                feature_group._get_feature_avro_schema(feature)
            )
            for feature in feature_group.get_complex_features()
        }

        # setup row writer function
        writer = self._get_encoder_func(feature_group._get_encoded_avro_schema())
        return producer, feature_writers, writer
