#
#   Copyright 2020 Logical Clocks AB
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
from hsfs.core import (
    feature_group_api,
    storage_connector_api,
    feature_group_internal_engine,
)


class OnDemandFeatureGroupEngine(
    feature_group_internal_engine.FeatureGroupInternalEngine
):
    def __init__(self, feature_store_id):
        super().__init__(feature_store_id)

        self._feature_group_api = feature_group_api.FeatureGroupApi(feature_store_id)
        self._storage_connector_api = storage_connector_api.StorageConnectorApi(
            feature_store_id
        )

    def save(self, feature_group):
        if len(feature_group.features) == 0:
            # If the user didn't specify the schema, parse it from the query
            on_demand_dataset = engine.get_instance().sql(
                feature_group.query, None, feature_group.connector, "default"
            )
            feature_group._features = engine.get_instance().parse_schema(
                on_demand_dataset
            )

        self._feature_group_api.save(feature_group)
