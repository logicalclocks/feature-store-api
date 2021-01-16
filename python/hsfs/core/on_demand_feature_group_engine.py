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
from hsfs.core import feature_group_base_engine


class OnDemandFeatureGroupEngine(feature_group_base_engine.FeatureGroupBaseEngine):
    def save(self, feature_group):
        if len(feature_group.features) == 0:
            # If the user didn't specify the schema, parse it from the query
            on_demand_dataset = (
                engine.get_instance().register_on_demand_temporary_table(
                    feature_group, "read_ondmd"
                )
            )
            feature_group._features = engine.get_instance().parse_schema_feature_group(
                on_demand_dataset
            )

        self._feature_group_api.save(feature_group)
