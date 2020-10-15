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

import datetime

from hsfs import engine, statistics
from hsfs.core import statistics_api
from hsfs.client import exceptions


class StatisticsEngine:
    def __init__(self, feature_store_id, entity_type):
        self._statistics_api = statistics_api.StatisticsApi(
            feature_store_id, entity_type
        )

    def compute_statistics(self, metadata_instance, feature_dataframe):
        """Compute statistics for a dataframe and send the result json to Hopsworks."""
        commit_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        if len(feature_dataframe.head(1)) == 0:
            raise exceptions.FeatureStoreException(
                "There is no data in the entity that you are trying to compute "
                "statistics for. A possible cause might be that you inserted only data "
                "to the online storage of a feature group."
            )
        content_str = engine.get_instance().profile(
            feature_dataframe,
            metadata_instance.statistics_config.columns,
            metadata_instance.statistics_config.correlations,
            metadata_instance.statistics_config.histograms,
        )
        stats = statistics.Statistics(commit_str, content_str)
        self._statistics_api.post(metadata_instance, stats)
        return stats

    def get_last(self, metadata_instance):
        """Get the most recent Statistics of an entity."""
        return self._statistics_api.get_last(metadata_instance)

    def get(self, metadata_instance, commit_time):
        """Get Statistics with the specified commit time of an entity."""
        return self._statistics_api.get(metadata_instance, commit_time)
