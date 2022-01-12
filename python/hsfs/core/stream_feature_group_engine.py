#
#   Copyright 2022 Logical Clocks AB
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

from hsfs import engine
from hsfs.client import exceptions
from hsfs.core import feature_group_engine


class StreamFeatureGroupEngine(feature_group_engine.FeatureGroupEngine):
    def __init__(self, feature_store_id):
        super().__init__(feature_store_id)

        # cache online feature store connector
        self._online_conn = None

    def insert(
        self,
        feature_group,
        feature_dataframe,
        overwrite,
        operation,
        storage,
        write_options,
    ):
        validation_id = None
        if feature_group.validation_type != "NONE" and engine.get_type() == "spark":
            # If the engine is Hive, the validation will be executed by
            # the Hopsworks job ingesting the data
            validation = feature_group.validate(feature_dataframe, True)
            validation_id = validation.validation_id

        offline_write_options = write_options
        online_write_options = self.get_kafka_config(write_options)

        if not feature_group.online_enabled and storage == "online":
            raise exceptions.FeatureStoreException(
                "Online storage is not enabled for this feature group."
            )

        if overwrite:
            self._feature_group_api.delete_content(feature_group)

        # create job for HUDI Deltastreamer
        self._feature_group_api.delta_streamer_job(feature_group, write_options)

        return engine.get_instance().save_dataframe(
            feature_group,
            feature_dataframe,
            "bulk_insert" if overwrite else operation,
            feature_group.online_enabled,
            storage,
            offline_write_options,
            online_write_options,
            validation_id,
        )
