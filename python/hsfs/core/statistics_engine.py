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

import json
import warnings
from typing import List, Union, Optional

from datetime import datetime, date

from hsfs import engine, statistics, util, split_statistics
from hsfs.client import exceptions
from hsfs.core import statistics_api, job
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics


class StatisticsEngine:
    def __init__(self, feature_store_id, entity_type):
        self._statistics_api = statistics_api.StatisticsApi(
            feature_store_id, entity_type
        )

    def compute_and_save_statistics(
        self,
        metadata_instance,
        feature_dataframe=None,
        feature_group_commit_id=None,
        feature_view_obj=None,
    ) -> Union[statistics.Statistics, job.Job]:
        """Compute statistics for a dataframe and send the result json to Hopsworks.
        Args:
            metadata_instance: Union[FeatureGroup, TrainingDataset]. Metadata of the entity containing the data.
            feature_dataframe: Spark or Pandas DataFrame to compute the statistics on.
            feature_group_commit_id: int. Feature group commit id.
            feature_view_obj: FeatureView. Metadata of the feature view, used when computing statistics for a Training Dataset.
        Returns:
            Union[Statistics, Job]. If running on Spark, statistics metadata containing a list of single feature descriptive statistics.
                                    Otherwise, Spark job metadata used to compute the statistics.
        """
        if engine.get_type().startswith("spark") or feature_view_obj is not None:
            # If the feature dataframe is None, then trigger a read on the metadata instance
            # We do it here to avoid making a useless request when using the Python engine
            # and calling compute_and_save_statistics
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

            computation_time = int(float(datetime.now().timestamp()) * 1000)
            stats_str = self.profile_statistics_with_config(
                feature_dataframe, metadata_instance.statistics_config
            )
            desc_stats = self._parse_deequ_statistics(stats_str)
            if desc_stats:
                stats = statistics.Statistics(
                    computation_time=computation_time,
                    feature_descriptive_statistics=desc_stats,
                    window_end_commit_time=feature_group_commit_id,
                )
                return self._save_statistics(stats, metadata_instance, feature_view_obj)
        else:
            # Python engine
            return engine.get_instance().profile_by_spark(metadata_instance)

    def compute_and_save_monitoring_statistics(
        self,
        metadata_instance,
        feature_dataframe,
        window_start_commit_time,
        window_end_commit_time,
        row_percentage,
        feature_name=None,
    ) -> statistics.Statistics:
        """Compute statistics for one or more features and send the result to Hopsworks.
        Args:
            metadata_instance: Union[FeatureGroup, TrainingDataset]. Metadata of the entity containing the data.
            feature_dataframe: Spark or Pandas DataFrame to compute the statistics on.
            window_start_commit_time: int. Window start commit time
            window_end_commit_time: int. Window end commit time
            row_percentage: float. Percentage of rows to include.
            feature_name: Optional[Union[str, List[str]]]. Feature name or list of names to compute the statistics on. If not set, statistics are computed on all features.
        Returns:
            Statistics. Statistics metadata containing a list of single feature descriptive statistics.
        """
        feature_names = []
        if feature_name is None:
            feature_names = feature_dataframe.columns
        elif isinstance(feature_name, str):
            feature_names = [feature_name]
        elif isinstance(feature_name, list):
            feature_names = feature_name

        if engine.get_type() == "spark":
            commit_time = int(float(datetime.now().timestamp()) * 1000)
            stats_str = self.profile_statistics(
                feature_dataframe, feature_names, False, False, False
            )
            desc_stats = self._parse_deequ_statistics(stats_str)

            stats = statistics.Statistics(
                computation_time=commit_time,
                row_percentage=row_percentage,
                feature_descriptive_statistics=desc_stats,
                window_end_commit_time=window_end_commit_time,
                window_start_commit_time=window_start_commit_time,
            )
            return self._save_statistics(stats, metadata_instance, None)
        else:
            # TODO: Only compute statistics with Spark at the moment. This method is expected to be called
            # only through run_feature_monitoring(), which is the entrypoint of the feature monitoring job.
            # Pending work for next sprint is to compute statistics on the Python client as well, as part of
            # the deequ replacement work.
            raise exceptions.FeatureStoreException(
                "Descriptive statistics for feature monitoring cannot be computed with the Python engine."
            )

    @staticmethod
    def profile_statistics_with_config(feature_dataframe, statistics_config) -> str:
        """Compute statistics on a feature DataFrame based on a given configuration.
        Args:
            feature_dataframe: Spark or Pandas DataFrame to compute the statistics on.
            statistics_config: StatisticsConfig. Configuration for the statistics to be computed.
        Returns:
            str. Serialized features statistics.
        """
        return StatisticsEngine.profile_statistics(
            feature_dataframe,
            statistics_config.columns,
            statistics_config.correlations,
            statistics_config.histograms,
            statistics_config.exact_uniqueness,
        )

    @staticmethod
    def profile_statistics(
        feature_dataframe, columns, correlations, histograms, exact_uniqueness
    ) -> str:
        """Compute statistics on a feature DataFrame.
        Args:
            feature_dataframe: Spark or Pandas DataFrame to compute the statistics on.
            columns: List[str]. List of feature names to compute the statistics on.
            correlations: bool. Whether to compute correlations or not.
            histograms: bool. Whether to compute histograms or not.
            exact_uniqueness: bool. Whether to compute exact uniqueness values or not.
        Returns:
            str. Serialized features statistics.
        """
        if len(feature_dataframe.head(1)) == 0:
            warnings.warn(
                "There is no data in the entity that you are trying to compute "
                "statistics for. A possible cause might be that you inserted only data "
                "to the online storage of a feature group.",
                category=util.StatisticsWarning,
            )
            # if empty data, set count to 0 and return
            col_stats = [{"column": col_name, "count": 0} for col_name in columns]
            return json.dumps({"columns": col_stats})
        return engine.get_instance().profile(
            feature_dataframe, columns, correlations, histograms, exact_uniqueness
        )

    def compute_and_save_split_statistics(
        self, td_metadata_instance, feature_view_obj=None, feature_dataframes=None
    ) -> statistics.Statistics:
        """Compute statistics on Training Dataset splits

        Args:
            td_metadata_instance: TrainingDataset. Training Dataset containing the splits.
            feature_view_obj: FeatureView. Metadata of the feature view used to create the Training Dataset. This parameter is optional.
            feature_dataframes: Spark or Pandas DataFrames containing the splits to compute the statistics on.

        Returns:
            Statistics. Statistics metadata containing a list of single feature descriptive statistics.
        """
        statistics_of_splits = []
        for split in td_metadata_instance.splits:
            split_name = split.name
            stats_str = self.profile_statistics_with_config(
                (
                    feature_dataframes.get(split_name)
                    if feature_dataframes
                    else td_metadata_instance.read(split_name)
                ),
                td_metadata_instance.statistics_config,
            )
            desc_stats = self._parse_deequ_statistics(stats_str)
            statistics_of_splits.append(
                split_statistics.SplitStatistics(
                    name=split_name,
                    feature_descriptive_statistics=desc_stats,
                )
            )
        computation_time = int(float(datetime.now().timestamp()) * 1000)
        stats = statistics.Statistics(
            computation_time=computation_time, split_statistics=statistics_of_splits
        )
        return self._save_statistics(stats, td_metadata_instance, feature_view_obj)

    def compute_transformation_fn_statistics(
        self,
        td_metadata_instance,
        columns,
        label_encoder_features,
        feature_dataframe=None,
        feature_view_obj=None,
    ) -> statistics.Statistics:
        """Compute statistics for transformation functions.
        Args:
            td_metadata_instance: TrainingDataset. Training Dataset containing the splits.
            columns: List[str]. List of feature names where transformation functions are applied, excluding label encoded features.
            label_encoder_features: List[str]. List of label encoded feature names.
            feature_dataframe: Spark or Pandas DataFrame to compute the statistics on. This parameter is optional.
            feature_view_obj: FeatureView. Metadata of the feature view used to create the Training Dataset. This parameter is optional.
        Returns:
            Statistics. Statistics metadata containing a list of single feature descriptive statistics.
        """
        computation_time = int(float(datetime.now().timestamp()) * 1000)
        stats_str = self._profile_transformation_fn_statistics(
            feature_dataframe, columns, label_encoder_features
        )
        desc_stats = self._parse_deequ_statistics(stats_str)
        stats = statistics.Statistics(
            computation_time=computation_time,
            feature_descriptive_statistics=desc_stats,
            before_transformation=True,
        )
        return self._save_statistics(stats, td_metadata_instance, feature_view_obj)

    def get(
        self,
        metadata_instance,
        feature_names: Optional[List[str]] = None,
        computation_time: Optional[Union[str, int, float, datetime, date]] = None,
        before_transformation: Optional[bool] = None,
        training_dataset_version: Optional[int] = None,
    ) -> Optional[statistics.Statistics]:
        """Get statistics of an entity computed at a specific time.
           If the computation time is not provided, the most recently computed statistics will be retrieved.

        Args:
            metadata_instance: Union[FeatureGroup, TrainingDataset]. Metadata of the entity containing the data.
            feature_names: List[str]. List of feature names of which statistics are retrieved.
            computation_time: Union[str, int, float, datetime, date]. Timestamp or computation time when statistics where computed.
            before_transformation: bool. Whether the statistics were computed before transformation functions or not.
            training_dataset_version: int. Version of the training dataset on which statistics were computed.
        Returns:
            Statistics. Statistics metadata containing a list of single feature descriptive statistics.
        """
        computation_timestamp = util.convert_event_time_to_timestamp(computation_time)
        try:
            return self._statistics_api.get(
                metadata_instance,
                feature_names=feature_names,
                computation_time=computation_timestamp,
                before_transformation=before_transformation,
                training_dataset_version=training_dataset_version,
            )
        except exceptions.RestAPIError as e:
            if (
                # statistics not found
                e.response.json().get("errorCode", "")
                == exceptions.RestAPIError.FeatureStoreErrorCode.STATISTICS_NOT_FOUND
                and e.response.status_code == 404
            ):
                return None
            raise e

    def get_all(
        self,
        metadata_instance,
        feature_names: Optional[List[str]] = None,
        computation_time: Optional[Union[str, int, float, datetime, date]] = None,
        training_dataset_version: Optional[int] = None,
    ) -> Optional[List[statistics.Statistics]]:
        """Get all statistics of an entity computed before a specific time.
           If the computation time is not provided, all the statistics will be retrieved.

        Args:
            metadata_instance: Union[FeatureGroup, TrainingDataset]. Metadata of the entity containing the data.
            feature_names: List[str]. List of feature names of which statistics are retrieved.
            computation_time: Union[str, int, float, datetime, date]. Timestamp or computation time when statistics where computed.
            training_dataset_version: int. Version of the training dataset on which statistics were computed.
        Returns:
            Statistics. Statistics metadata containing a list of single feature descriptive statistics.
        """
        try:
            return self._statistics_api.get_all(
                metadata_instance,
                feature_names=feature_names,
                computation_time=computation_time,
                training_dataset_version=training_dataset_version,
            )
        except exceptions.RestAPIError as e:
            if (
                # statistics not found
                e.response.json().get("errorCode", "")
                == exceptions.RestAPIError.FeatureStoreErrorCode.STATISTICS_NOT_FOUND
                and e.response.status_code == 404
            ):
                return None
            raise e

    def get_by_time_window(
        self,
        metadata_instance,
        start_commit_time: Optional[Union[str, int, datetime, date]] = None,
        end_commit_time: Optional[Union[str, int, datetime, date]] = None,
        feature_names: Optional[List[str]] = None,
        row_percentage: Optional[float] = None,
    ) -> Union[statistics.Statistics, List[statistics.Statistics], None]:
        """Get the statistics of an entity based on a commit time window.
        Args:
            metadata_instance: Union[FeatureGroup]: Metadata of the entity containing the data.
            start_commit_time: int: Window start commit time
            end_commit_time: int: Window end commit time
            feature_names: List[str]. List of feature names of which statistics are retrieved.
            row_percentage: float. Percentage of feature values used during statistics computation
        Returns:
            Statistics:  Statistics metadata containing a list of single feature descriptive statistics.
        """
        start_commit_time = util.convert_event_time_to_timestamp(start_commit_time)
        end_commit_time = util.convert_event_time_to_timestamp(end_commit_time)
        try:
            return self._statistics_api.get(
                metadata_instance,
                start_commit_time=start_commit_time,
                end_commit_time=end_commit_time,
                feature_names=feature_names,
                row_percentage=row_percentage,
            )
        except exceptions.RestAPIError as e:
            if (
                # statistics not found
                e.response.json().get("errorCode", "")
                == exceptions.RestAPIError.FeatureStoreErrorCode.STATISTICS_NOT_FOUND
                and e.response.status_code == 404
            ) or (
                # commits not found
                e.response.json().get("errorCode", "")
                == exceptions.RestAPIError.FeatureStoreErrorCode.FEATURE_GROUP_COMMIT_NOT_FOUND
                and e.response.status_code == 400
            ):
                return None
            raise e

    def _profile_transformation_fn_statistics(
        self, feature_dataframe, columns, label_encoder_features
    ) -> str:
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

        # compute statistics for all features with transformation fn
        all_columns = (columns or []) + (label_encoder_features or [])
        stats_str = engine.get_instance().profile(
            feature_dataframe, all_columns, False, True, False
        )

        # add unique values profile to column stats
        return self._profile_unique_values(
            feature_dataframe, label_encoder_features, stats_str
        )

    def _profile_unique_values(
        self, feature_dataframe, label_encoder_features, stats_str
    ) -> str:
        stats = json.loads(stats_str)
        if not stats:
            stats = {"columns": []}
        stats_dict = {col_stats["column"]: col_stats for col_stats in stats["columns"]}
        for column in label_encoder_features:
            col_stats_unique_values = {
                "column": column,
                "unique_values": [
                    value
                    for value in engine.get_instance().get_unique_values(
                        feature_dataframe, column
                    )
                ],
            }
            if column in stats_dict:
                stats_dict[column].update(col_stats_unique_values)
            else:
                stats_dict[column] = col_stats_unique_values

        stats["columns"] = list(stats_dict.values())
        return json.dumps(stats)  # the result is a JSON string

    def _save_statistics(
        self, stats, metadata_instance, feature_view_obj
    ) -> statistics.Statistics:
        # metadata_instance can be feature group or training dataset
        if feature_view_obj:
            stats = self._statistics_api.post(
                feature_view_obj,
                stats=stats,
                training_dataset_version=metadata_instance.version,
            )
        else:
            stats = self._statistics_api.post(
                metadata_instance, stats=stats, training_dataset_version=None
            )
        return stats

    def _parse_deequ_statistics(self, stats) -> List[FeatureDescriptiveStatistics]:
        if stats is None:
            warnings.warn(
                "There is no Deequ statistics to deserialize. A possible cause might be that Deequ did not succeed in the statistics computation.",
                category=util.StatisticsWarning,
            )
            return None
        if isinstance(stats, str):
            stats = json.loads(stats)
        return [
            FeatureDescriptiveStatistics.from_deequ_json(col_stats)
            for col_stats in stats["columns"]
        ]
