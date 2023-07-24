#
#   Copyright 2023 Hopsworks AB
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

from hsfs import engine, util
from hsfs.client.exceptions import FeatureStoreException
from hsfs.core import feature_group_base_engine


class SpineGroupEngine(feature_group_base_engine.FeatureGroupBaseEngine):
    def save(self, feature_group):
        if feature_group.features is None or len(feature_group.features) == 0:
            # if python engine user should pass features as we do not parse it in this case
            if feature_group.dataframe is None:
                raise FeatureStoreException(
                    "Features (schema) need to be set for creation of spine feature groups with engine "
                    + engine.get_type()
                    + ". Alternatively use Spark kernel."
                )

            feature_group._features = engine.get_instance().parse_schema_feature_group(
                feature_group.dataframe
            )

        # set primary and partition key columns
        # we should move this to the backend
        util.verify_attribute_key_names(feature_group, True)
        for feat in feature_group.features:
            if feat.name in feature_group.primary_key:
                feat.primary = True

        # need to save dataframe during save since otherwise it will be lost
        dataframe = feature_group.dataframe
        self._feature_group_api.save(feature_group)
        feature_group.dataframe = dataframe
