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
from typing import List, TypeVar
import warnings

from hsfs import engine, statistics, util, split_statistics
from hsfs.client import exceptions
from hsfs.core import statistics_api
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics


class StatisticsEngine:
    def __init__(self, feature_store_id, entity_type):
        self._statistics_api = statistics_api.StatisticsApi(
            feature_store_id, entity_type
        )

    def compute_statistics(
        self,
        metadata_instance,
        features_dataframe=None,
        feature_group_commit_id=None,
        feature_view_obj=None,
    ):
        """Compute statistics for a dataframe and send the result json to Hopsworks.

        Args:
            metadata_instance: Union[FeatureGroup, TrainingDataset] Metadata of the entity containing the data.
            features_dataframe: Spark or Pandas DataFrame to compute the statistics on.
            feature_group_commit_id: int. Feature group commit id.
            feature_view_obj: Metadata of the feature view, used when computing statistics for a Training Dataset.
        """
        if engine.get_type() == "spark" or feature_view_obj is not None:
            # If the feature dataframe is None, then trigger a read on the metadata instance
            # We do it here to avoid making a useless request when using the Python engine
            # and calling compute_statistics
            if features_dataframe is None:
                if feature_group_commit_id is not None:
                    features_dataframe = (
                        metadata_instance.select_all()
                        .as_of(
                            util.get_hudi_datestr_from_timestamp(
                                feature_group_commit_id
                            )
                        )
                        .read(online=False, dataframe_type="default", read_options={})
                    )
                else:
                    features_dataframe = metadata_instance.read()

            commit_time = int(float(datetime.datetime.now().timestamp()) * 1000)

            content_str = self.profile_statistics_with_config(
                features_dataframe, metadata_instance.statistics_config
            )
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

    def compute_monitoring_statistics(
        self,
        feature_dataframe: TypeVar("pyspark.sql.DataFrame"),
    ) -> List[FeatureDescriptiveStatistics]:
        """Compute statistics for a DataFrame without sending the result to Hopsworks.

        Args:
            feature_dataframe: DataFrame to compute the statistics on.

        Returns:
            List[FeatureDescriptiveStatistics]. List of the Descriptive statistics
                for each feature in the DataFrame.
        """

        # TODO: Future work. Persisting the statistics and returning the stats together with the ID
        if engine.get_type() == "spark":
            if feature_dataframe is not None:
                feature_names = feature_dataframe.schema.name
            else:
                feature_names = []
            statistics_str = self.profile_statistics(
                feature_dataframe, feature_names, False, False, False
            )
            statistics_dict = json.loads(statistics_str)

            return [
                FeatureDescriptiveStatistics.from_deequ_json(
                    statistics_dict[feature_name]
                )
                for feature_name in feature_names
            ]
        else:
            # TODO: Only compute statistics with Spark at the moment. This method is expected to be called
            # only through run_feature_monitoring(), which is the entrypoint of the feature monitoring job.
            # Pending work for next sprint is to compute statistics on the Python client as well, as part of
            # the deequ replacement work.
            raise exceptions.FeatureStoreException(
                "Descriptive statistics for feature monitoring cannot be computed from the Python engine."
            )

    @staticmethod
    def profile_statistics_with_config(features_dataframe, statistics_config):
        return StatisticsEngine.profile_statistics(
            features_dataframe,
            statistics_config.columns,
            statistics_config.correlations,
            statistics_config.histograms,
            statistics_config.exact_uniqueness,
        )

    @staticmethod
    def profile_statistics(
        features_dataframe, columns, correlations, histograms, exact_uniqueness
    ):
        if len(features_dataframe.head(1)) == 0:
            warnings.warn(
                "There is no data in the entity that you are trying to compute "
                "statistics for. A possible cause might be that you inserted only data "
                "to the online storage of a feature group.",
                category=util.StatisticsWarning,
            )
            return "{}"
        return engine.get_instance().profile(
            features_dataframe, columns, correlations, histograms, exact_uniqueness
        )

    def profile_transformation_fn_statistics(
        self, features_dataframe, columns, label_encoder_features
    ):
        if (
            engine.get_type() == "spark"
            and len(features_dataframe.select(*columns).head(1)) == 0
        ) or (
            (engine.get_type() == "hive" or engine.get_type() == "python")
            and len(features_dataframe.head()) == 0
        ):
            raise exceptions.FeatureStoreException(
                "There is no data in the entity that you are trying to compute "
                "statistics for. A possible cause might be that you inserted only data "
                "to the online storage of a feature group."
            )
        content_str = engine.get_instance().profile(
            features_dataframe, columns, False, True, False
        )

        # add unique value profile to String type columns
        return self.profile_unique_values(
            features_dataframe, label_encoder_features, content_str
        )

    def register_split_statistics(
        self, td_metadata_instance, feature_view_obj=None, feature_dataframes=None
    ):
        statistics_of_splits = []
        for split in td_metadata_instance.splits:
            split_name = split.name
            stat_content = self.profile_statistics_with_config(
                (
                    feature_dataframes.get(split_name)
                    if feature_dataframes
                    else td_metadata_instance.read(split_name)
                ),
                td_metadata_instance.statistics_config,
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
        features_dataframe=None,
        feature_view_obj=None,
    ):
        commit_time = int(float(datetime.datetime.now().timestamp()) * 1000)
        content_str = self.profile_transformation_fn_statistics(
            features_dataframe, columns, label_encoder_features
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

    def get_by_feature_name_time_window_and_row_percentage(
        self,
        metadata_instance,
        feature_name: str,
        start_time: int,
        end_time: int,
        row_percentage: int,
    ) -> FeatureDescriptiveStatistics:
        """Get feature statistics based on commit time window and row percentage

        Args:
            metadata_instance: Union[FeatureGroup, FeatureView]: Entity on which statistics where computed.
            feature_name: str: Name of the feature from which statistics where computed.
            start_time: Window start commit time
            end_time: Window end commit time
            row_percentage: Percentage of rows used in the computation of statitics

        Returns:
            FeatureDescriptiveStatistics: Descriptive statistics
        """
        return self._statistics_api.get_by_feature_name_time_window_and_row_percentage(
            metadata_instance, feature_name, start_time, end_time, row_percentage
        )

    def _save_statistics(self, stats, td_metadata_instance, feature_view_obj):
        if feature_view_obj:
            self._statistics_api.post(
                feature_view_obj, stats, td_metadata_instance.version
            )
        else:
            self._statistics_api.post(td_metadata_instance, stats, None)

    @staticmethod
    def profile_unique_values(features_dataframe, label_encoder_features, content_str):
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
                        features_dataframe, column
                    )
                ],
            }
            content_dict["columns"].append(unique_values)
        # the result is a JSON string:
        return json.dumps(content_dict)
