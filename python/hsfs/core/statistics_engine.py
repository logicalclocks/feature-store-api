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
import json
import warnings

from hsfs import engine, statistics, util, split_statistics
from hsfs.client import exceptions
from hsfs.core import statistics_api


class StatisticsEngine:
    def __init__(self, feature_store_id, entity_type):
        self._statistics_api = statistics_api.StatisticsApi(
            feature_store_id, entity_type
        )

    def compute_statistics(
        self,
        metadata_instance,
        feature_dataframe=None,
        feature_group_commit_id=None,
        feature_view_obj=None,
    ):
        """Compute statistics for a dataframe and send the result json to Hopsworks."""
        if engine.get_type() == "spark" or feature_view_obj is not None:
            # If the feature dataframe is None, then trigger a read on the metadata instance
            # We do it here to avoid making a useless request when using the Python engine
            # and calling compute_statistics
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

            content_str = self.profile_statistics(metadata_instance, feature_dataframe)
            if content_str:
                stats = statistics.Statistics(
                    commit_time=commit_time,
                    content=content_str,
                    feature_group_commit_id=feature_group_commit_id,
                )
                self._save_statistics(stats, metadata_instance, feature_view_obj)
        else:
            # Python engine
            engine.get_instance().profile_by_spark(metadata_instance)

    @staticmethod
    def profile_statistics(metadata_instance, feature_dataframe):
        if len(feature_dataframe.head(1)) == 0:
            warnings.warn(
                "There is no data in the entity that you are trying to compute "
                "statistics for. A possible cause might be that you inserted only data "
                "to the online storage of a feature group.",
                category=util.StatisticsWarning,
            )
            return "{}"
        return engine.get_instance().profile(
            feature_dataframe,
            metadata_instance.statistics_config.columns,
            metadata_instance.statistics_config.correlations,
            metadata_instance.statistics_config.histograms,
            metadata_instance.statistics_config.exact_uniqueness,
        )

    def profile_transformation_fn_statistics(
        self, feature_dataframe, columns, label_encoder_features
    ):
        if (
            engine.get_type() == "spark"
            and len(feature_dataframe.select(*columns).head(1)) == 0
        ) or (
            (engine.get_type() == "hive" or engine.get_type() == "python")
            and len(feature_dataframe.head()) == 0
        ):
            raise exceptions.FeatureStoreException(
                "There is no data in the entity that you are trying to compute "
                "statistics for. A possible cause might be that you inserted only data "
                "to the online storage of a feature group."
            )
        content_str = engine.get_instance().profile(
            feature_dataframe, columns, False, True, False
        )

        # add unique value profile to String type columns
        return self.profile_unique_values(
            feature_dataframe, label_encoder_features, content_str
        )

    def register_split_statistics(
        self, td_metadata_instance, feature_view_obj=None, feature_dataframes=None
    ):
        statistics_of_splits = []
        for split in td_metadata_instance.splits:
            split_name = split.name
            stat_content = self.profile_statistics(
                td_metadata_instance,
                (
                    feature_dataframes.get(split_name)
                    if feature_dataframes
                    else td_metadata_instance.read(split_name)
                ),
            )
            statistics_of_splits.append(
                split_statistics.SplitStatistics(
                    split_name,
                    stat_content if stat_content else "{}",
                )
            )

        commit_time = int(float(datetime.datetime.now().timestamp()) * 1000)
        stats = statistics.Statistics(
            commit_time=commit_time, split_statistics=statistics_of_splits
        )

        self._save_statistics(stats, td_metadata_instance, feature_view_obj)
        return stats

    def compute_transformation_fn_statistics(
        self,
        td_metadata_instance,
        columns,
        label_encoder_features,
        feature_dataframe=None,
        feature_view_obj=None,
    ):
        commit_time = int(float(datetime.datetime.now().timestamp()) * 1000)
        content_str = self.profile_transformation_fn_statistics(
            feature_dataframe, columns, label_encoder_features
        )
        stats = statistics.Statistics(
            commit_time=commit_time,
            content=content_str,
            for_transformation=True,
        )
        self._save_statistics(stats, td_metadata_instance, feature_view_obj)
        return stats

    def get_last(
        self, metadata_instance, for_transformation=False, training_dataset_version=None
    ):
        """Get the most recent Statistics of an entity."""
        return self._statistics_api.get_last(
            metadata_instance, for_transformation, training_dataset_version
        )

    def get(
        self,
        metadata_instance,
        commit_time,
        for_transformation=False,
        training_dataset_version=None,
    ):
        """Get Statistics with the specified commit time of an entity."""
        commit_timestamp = util.convert_event_time_to_timestamp(commit_time)
        return self._statistics_api.get(
            metadata_instance,
            commit_timestamp,
            for_transformation,
            training_dataset_version,
        )

    def _save_statistics(self, stats, td_metadata_instance, feature_view_obj):
        if feature_view_obj:
            self._statistics_api.post(
                feature_view_obj, stats, td_metadata_instance.version
            )
        else:
            self._statistics_api.post(td_metadata_instance, stats, None)

    @staticmethod
    def profile_unique_values(feature_dataframe, label_encoder_features, content_str):
        # parsing JSON string:
        content_dict = json.loads(content_str)
        if not content_dict:
            content_dict = {"columns": []}
        for column in label_encoder_features:
            unique_values = {
                "column": column,
                "unique_values": [
                    value
                    for value in engine.get_instance().get_unique_values(
                        feature_dataframe, column
                    )
                ],
            }
            content_dict["columns"].append(unique_values)
        # the result is a JSON string:
        return json.dumps(content_dict)
