#
#   Copyright 2020 Logical Clocks AB
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

# from hsfs import engine
from hsfs.core import feature_group_api


class FeatureGroupEngine:
    OVERWRITE = "overwrite"
    APPEND = "append"

    def __init__(self, feature_store_id):
        self._feature_group_api = feature_group_api.FeatureGroupApi(feature_store_id)

    def save(self, feature_group, feature_dataframe):
        # set primary and partition key columns
        # we should move this to the backend
        for feat in feature_group.features:
            if feat.name in feature_group.primary_key:
                feat.primary = True
            if feat.name in feature_group.partition_key:
                feat.partition = True

        self._feature_group_api.post(feature_group)

        table_name = self._get_table_name(feature_group)
        print(table_name)

        # engine.get_instance().save(feature_dataframe, table_name, feature_group.partition_key, self.APPEND)

    def insert(self):
        raise NotImplementedError

    def _get_table_name(self, feature_group):
        return (
            feature_group.feature_store_name
            + "."
            + feature_group.name
            + "_"
            + feature_group.version
        )
