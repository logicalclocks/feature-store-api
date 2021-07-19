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

import datetime

from hsfs import engine, code, util
from hsfs.core import code_api
from hsfs.client import exceptions
import os


class CodeEngine:
    def __init__(self, feature_store_id, entity_type):
        self._code_api = code_api.CodeApi(
            feature_store_id, entity_type
        )

    def compute_code(
        self, metadata_instance, feature_dataframe=None, feature_group_commit_id=None
    ):
        """Compute code for a dataframe and send the result json to Hopsworks."""
        if engine.get_type() == "spark":

            # If the feature dataframe is None, then trigger a read on the metadata instance
            # We do it here to avoid making a useless request when using the Hive engine
            # and calling compute_code
            if feature_dataframe is None:
                if feature_group_commit_id is not None:
                    feature_dataframe = (
                        metadata_instance.select_all()
                        .as_of(
                            util.get_hudi_datestr_from_timestamp(
                                feature_group_commit_id
                            )
                        )
                        .read(online=False, dataframe_type="default", read_options={})
                    )
                else:
                    feature_dataframe = metadata_instance.read()

            commit_time = int(float(datetime.datetime.now().timestamp()) * 1000)
            if len(feature_dataframe.head(1)) == 0:
                raise exceptions.FeatureStoreException(
                    "There is no data in the entity that you are trying to compute "
                    "code for. A possible cause might be that you inserted only data "
                    "to the online storage of a feature group."
                )
            code_entity = code.Code(
                commit_time=commit_time,
                feature_group_commit_id=feature_group_commit_id,
            )
            self._code_api.post(metadata_instance, code_entity, os.environ["HOPSWORKS_KERNEL_ID"], "JUPYTER")
            return code_entity

        else:
            # Hive engine
            engine.get_instance().profile(metadata_instance)
