#
#   Copyright 2023 Hopsworks AB
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
from typing import Any, Dict, List, Optional, Union
from datetime import date, datetime, timedelta
import re

from hsfs.core import feature_monitoring_config_api
from hsfs.core import feature_monitoring_config as fmc
from hsfs.core.feature_monitoring_result_engine import FeatureMonitoringResultEngine
from hsfs.core.statistics_engine import StatisticsEngine
from hsfs.core.feature_monitoring_result import FeatureMonitoringResult
from hsfs.core.monitoring_window_config import (
    MonitoringWindowConfig,
    WindowConfigType,
)
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics

from hsfs.core.job import Job
from hsfs.core.job_api import JobApi
from hsfs.core.job_scheduler import JobScheduler
from hsfs.util import convert_event_time_to_timestamp
from hsfs.client.exceptions import FeatureStoreException


class FeatureMonitoringConfigEngine:
    """Logic and helper methods to perform feature monitoring based on a given configuration.

    Attributes:
        feature_store_id: int. Id of the respective Feature Store.
        feature_group_id: int. Id of the feature group, if monitoring a feature group.
        feature_view_id: int. Id of the feature view, if monitoring a feature view.
        feature_view_name: str. Name of the feature view, if monitoring a feature view.
        feature_view_version: int. Version of the feature view, if monitoring a feature view.
    """

    def __init__(
        self,
        feature_store_id: int,
        feature_group_id: Optional[int] = None,
        feature_view_id: Optional[int] = None,
        feature_view_name: Optional[str] = None,
        feature_view_version: Optional[int] = None,
    ) -> None:
        self._feature_store_id = feature_store_id
        self._feature_group_id = feature_group_id
        self._feature_view_id = feature_view_id
        self._feature_view_name = feature_view_name
        self._feature_view_version = feature_view_version

        if feature_group_id is None:
            assert feature_view_id is not None
            assert feature_view_name is not None
            assert feature_view_version is not None
            entity_type = "featuregroups"
        else:
            entity_type = "featureview"

        self._feature_monitoring_config_api = (
            feature_monitoring_config_api.FeatureMonitoringConfigApi(
                feature_store_id=feature_store_id,
                feature_group_id=feature_group_id,
                feature_view_name=feature_view_name,
                feature_view_version=feature_view_version,
            )
        )
        self._statistics_engine = StatisticsEngine(feature_store_id, entity_type)

        # Should we promote the variables below to static?
        self._VALID_CATEGORICAL_METRICS = [
            "completeness",
            "numRecordsNonNull",
            "numRecordsNull",
            "distinctness",
            "entropy",
            "uniqueness",
            "approximateNumDistinctValues",
            "exactNumDistinctValues",
        ]
        self._VALID_CATEGORICAL_METRICS = [
            metric.lower() for metric in self._VALID_CATEGORICAL_METRICS
        ]
        self._VALID_FRACTIONAL_METRICS = [
            "completeness",
            "numRecordsNonNull",
            "numRecordsNull",
            "distinctness",
            "entropy",
            "uniqueness",
            "approximateNumDistinctValues",
            "exactNumDistinctValues",
            "mean",
            "maximum",
            "minimum",
            "sum",
            "stdDev",
            "count",
        ]
        self._VALID_FRACTIONAL_METRICS = [
            metric.lower() for metric in self._VALID_FRACTIONAL_METRICS
        ]
        self._DEFAULT_ROW_PERCENTAGE = 20

    def enable_descriptive_statistics_monitoring(
        self,
        name: str,
        feature_name: str,
        detection_window_config: MonitoringWindowConfig,
        scheduler_config: Optional[Union[JobScheduler, Dict[str, Any]]] = None,
        description: Optional[str] = None,
    ) -> "fmc.FeatureMonitoringConfig":
        """Enable descriptive statistics monitoring for a feature.

        Args:
            name: str, required
                Name of the monitoring configuration.
            feature_name: str, required
                Name of the feature to monitor.
            detection_window_config: MonitoringWindowConfig, required
                Configuration of the detection window.
            scheduler_config: Union[JobScheduler, Dict[str, Any]], optional
                Configuration of the scheduler.
            description: str, optional
                Description of the monitoring configuration.

        Returns:
            FeatureMonitoringConfig
        """
        config = self._build_stats_monitoring_only_config(
            name=name,
            feature_name=feature_name,
            detection_window_config=detection_window_config,
            scheduler_config=scheduler_config,
            description=description,
        )

        return self._feature_monitoring_config_api.create(
            fm_config=config,
        )

    def enable_feature_monitoring_config(
        self,
        feature_name: str,
        name: str,
        detection_window_config: MonitoringWindowConfig,
        reference_window_config: MonitoringWindowConfig,
        statistics_comparison_config: Dict[str, Any],
        alert_config: str,
        scheduler_config: Optional[Union[JobScheduler, Dict[str, Any]]] = None,
        description: Optional[str] = None,
    ) -> "fmc.FeatureMonitoringConfig":
        """Enable feature monitoring for a feature.

        Args:
            feature_name: str, required
                Name of the feature to monitor.
            name: str, required
                Name of the monitoring configuration.
            detection_window_config: MonitoringWindowConfig, required
                Configuration of the detection window.
            reference_window_config: MonitoringWindowConfig, required
                Configuration of the reference window.
            statistics_comparison_config: Dict[str, Any], required
                Configuration of the statistics comparison.
            alert_config: str, optional
                Configuration of the alert.
            scheduler_config: Union[JobScheduler, Dict[str, Any]], optional
                Configuration of the scheduler.
            description: str, optional
                Description of the monitoring configuration.

        Returns:
            FeatureMonitoringConfig The registered monitoring configuration.
        """
        config = self._build_feature_monitoring_config(
            name=name,
            feature_name=feature_name,
            detection_window_config=detection_window_config,
            reference_window_config=reference_window_config,
            statistics_comparison_config=statistics_comparison_config,
            scheduler_config=scheduler_config,
            alert_config=alert_config,
            description=description,
        )

        return self._feature_monitoring_config_api.create(
            fm_config=config,
        )

    def build_monitoring_window_config(
        self,
        window_config_type: str,
        id: Optional[int] = None,
        time_offset: Optional[str] = None,
        window_length: Optional[str] = None,
        specific_id: Optional[int] = None,
        specific_value: Optional[float] = None,
        row_percentage: Optional[int] = None,
    ) -> MonitoringWindowConfig:
        """Builds a monitoring window config.

        Args:
            window_config_type: str, required
                Type of the window config, can be either
                `SPECIFIC_VALUE`,`FEATURE_GROUP`,`INSERT`,`BATCH` or `FEATURE_VIEW`.
            time_offset: str, optional
                monitoring window start time is computed as "now - time_offset".
            window_length: str, optional
                monitoring window end time is computed as
                    "now - time_offset + window_length".
            specific_id: int, optional
                Specific id of an entity that has fixed statistics.
            specific_value: float, optional
                Specific value instead of a statistics computed on data.

        Returns:
            MonitoringWindowConfig The monitoring window configuration.
        """

        return MonitoringWindowConfig(
            id=id,
            window_config_type=window_config_type,
            time_offset=time_offset,
            window_length=window_length,
            specific_id=specific_id,
            specific_value=specific_value,
            row_percentage=row_percentage,
        )

    def validate_statistics_comparison_config(
        self,
        metric: str,
        threshold: float,
        id: Optional[int] = None,
        relative: bool = False,
        strict: bool = False,
    ) -> Dict[str, Any]:
        """Validates the statistics comparison config.

        Args:
            metric: str, required
                Statistical metric to perform comparison on.
            threshold: float, required
                If statistics difference is above threshold, trigger an alert if configured.
            relative: bool, optional
                If true, compute the relative difference between the detection and reference statistics.
                Defaults to false.
            strict: bool, optional
                If true, the statistics difference must be strictly above threshold to trigger an alert.
                Defaults to false.

        Raises:
            ValueError: If the statistics comparison config is invalid.
        """
        # TODO: Add more validation logic based on detection and reference window config.
        if (
            metric.lower() not in self._VALID_CATEGORICAL_METRICS
            and metric.lower() not in self._VALID_FRACTIONAL_METRICS
        ):
            raise ValueError(
                f"Invalid metric {metric.lower()}. "
                "Supported metrics are {}.".format(
                    set(self._VALID_FRACTIONAL_METRICS).union(
                        set(self._VALID_CATEGORICAL_METRICS)
                    )
                )
            )
        else:
            return {
                "id": id,
                "metric": metric.upper(),
                "threshold": threshold,
                "relative": relative,
                "strict": strict,
            }

    def build_job_scheduler(
        self,
        job_frequency: Optional[str] = "DAILY",
        start_date_time: Optional[Union[str, int, date, datetime]] = None,
        job_name: Optional[str] = None,
        enabled: Optional[bool] = True,
        id: Optional[int] = None,
    ) -> JobScheduler:
        """Builds a job scheduler.

        Args:
            job_frequency: str, required
                Frequency of the job. Defaults to daily.
            start_date_time: Union[str, int, date, datetime], optional
                Job will start being executed on schedule from that time.
                Defaults to datetime.now().
            job_name: str, optional
                Name of the job. Populated when registering the feature monitoring
                configuration to the backend. Defaults to None.
            enabled: bool, optional
                If enabled is false, the scheduled job is not executed.
                Defaults to True.
            id: int, optional
                Id of the job scheduler. Populated when registering the feature monitoring
                configuration to the backend. Defaults to None.

        Returns:
            JobScheduler The job scheduler.
        """
        if start_date_time is None:
            start_date_time = convert_event_time_to_timestamp(datetime.now())
        else:
            start_date_time = convert_event_time_to_timestamp(start_date_time)

        if job_frequency.upper() not in ["HOURLY", "DAILY", "WEEKLY"]:
            raise ValueError(
                "Invalid job frequency. Supported frequencies are HOURLY, DAILY, WEEKLY."
            )

        return JobScheduler(
            id=id,
            job_frequency=job_frequency,
            start_date_time=start_date_time,
            job_name=job_name,
            enabled=enabled,
        )

    def validate_config_name(self, name: str):
        if not isinstance(name, str):
            raise TypeError("Invalid config name. Config name must be a string.")
        if len(name) > 64:
            raise ValueError(
                "Invalid config name. Config name must be less than 64 characters."
            )
        if not re.match(r"^[\w]*$", name):
            raise ValueError(
                "Invalid config name. Config name must be alphanumeric or underscore."
            )

    def validate_description(self, description: Optional[str]):
        if description is not None and not isinstance(description, str):
            raise TypeError("Invalid description. Description must be a string.")
        if description is not None and len(description) > 256:
            raise ValueError(
                "Invalid description. Description must be less than 256 characters."
            )

    def validate_feature_name(
        self, feature_name: Optional[str], valid_feature_names: Optional[List[str]]
    ):
        if feature_name is not None and not isinstance(feature_name, str):
            raise TypeError("Invalid feature name. Feature name must be a string.")
        if feature_name is not None and feature_name not in valid_feature_names:
            raise ValueError(
                f"Invalid feature name. Feature name must be one of {valid_feature_names}."
            )

    def _build_default_scheduled_statistics_config(
        self,
        name: str,
        feature_name: Optional[str] = None,
        job_frequency: Optional[str] = "DAILY",
        start_date_time: Optional[Union[str, int, date, datetime]] = None,
        description: Optional[str] = None,
        valid_feature_names: Optional[List[str]] = None,
    ) -> "fmc.FeatureMonitoringConfig":
        """Builds the default scheduled statistics config, default detection window is full snapshot.

        Args:
            name: str, required
                Name of the feature monitoring configuration, must be unique for
                the feature view or feature group.
            feature_name: str, optional
                If provided, compute statistics only for this feature. If none,
                defaults, compute statistics for all features.
            job_frequency: str, optional
                defines how often the statistics should be computed. Defaults to daily.
            start_date_time: Union[str, int, date, datetime], optional
                Statistics will start being computed on schedule from that time.
            description: str, optional
                Description of the feature monitoring configuration.
            valid_feature_names: List[str], optional
                List of the feature names for the feature view or feature group.

        Returns:
            FeatureMonitoringConfig A Feature Monitoring Configuration to compute
              the statistics of a snapshot of all data present in the entity.
        """
        self.validate_config_name(name)
        self.validate_description(description)
        if feature_name is not None:
            self.validate_feature_name(feature_name, valid_feature_names)

        return fmc.FeatureMonitoringConfig(
            feature_store_id=self._feature_store_id,
            feature_group_id=self._feature_group_id,
            feature_view_id=self._feature_view_id,
            feature_view_name=self._feature_view_name,
            feature_view_version=self._feature_view_version,
            name=name,
            description=description,
            feature_name=feature_name,
            feature_monitoring_type="SCHEDULED_STATISTICS",
            scheduler_config={
                "job_frequency": job_frequency,
                "start_date_time": start_date_time,
                "enabled": True,
            },
        ).with_detection_window(
            window_config_type="FULL_SNAPSHOT",
            row_percentage=self._DEFAULT_ROW_PERCENTAGE,
        )

    def _build_default_feature_monitoring_config(
        self,
        name: str,
        feature_name: str,
        job_frequency: Optional[str] = "DAILY",
        start_date_time: Optional[Union[str, int, date, datetime]] = None,
        description: Optional[str] = None,
        valid_feature_names: Optional[List[str]] = None,
    ) -> "fmc.FeatureMonitoringConfig":
        """Builds the default scheduled statistics config, default detection window is full snapshot.

        Args:
            name: str, required
                Name of the feature monitoring configuration, must be unique for
                the feature view or feature group.
            feature_name: str, optional
                If provided, compute statistics only for this feature. If none,
                defaults, compute statistics for all features.
            job_frequency: str, optional
                defines how often the statistics should be computed. Defaults to daily.
            start_date_time: Union[str, int, date, datetime], optional
                Statistics will start being computed on schedule from that time.
            description: str, optional
                Description of the feature monitoring configuration.
            valid_feature_names: List[str], optional
                List of the feature names for the feature view or feature group.

        Returns:
            FeatureMonitoringConfig A Feature Monitoring Configuration to compute
              the statistics of a snapshot of all data present in the entity.
        """
        self.validate_feature_name(feature_name, valid_feature_names)
        self.validate_config_name(name)
        self.validate_description(description)

        return fmc.FeatureMonitoringConfig(
            feature_store_id=self._feature_store_id,
            feature_group_id=self._feature_group_id,
            feature_view_id=self._feature_view_id,
            feature_view_name=self._feature_view_name,
            feature_view_version=self._feature_view_version,
            name=name,
            description=description,
            feature_name=feature_name,
            # setting feature_monitoring_type to "SCHEDULED_STATISTICS" allows
            # to raise an error if no reference window and comparison config are provided
            feature_monitoring_type="DESCRIPTIVE_STATISTICS",
            job_scheduler={
                "job_frequency": job_frequency,
                "start_date_time": start_date_time,
                "enabled": True,
            },
        ).with_detection_window(
            window_config_type="FULL_SNAPSHOT",
            row_percentage=self._DEFAULT_ROW_PERCENTAGE,
        )  # TODO: Do we want to have a default reference window + stat comparison?

    def _build_stats_monitoring_only_config(
        self,
        name: str,
        feature_name: str,
        detection_window_config: Dict[str, Any],
        scheduler_config: Optional[Union[JobScheduler, Dict[str, Any]]] = None,
        description: Optional[str] = None,
    ) -> "fmc.FeatureMonitoringConfig":
        """Builds a feature monitoring config for descriptive statistics only.

        Args:
            name: str, required
                Name of the monitoring configuration.
            feature_name: str, required
                Name of the feature to monitor.
            detection_window_config: Dict[str, Any], required
                Configuration of the detection window.
            scheduler_config: Optional[Union[JobScheduler, Dict[str, Any]]], optional
                Configuration of the scheduler.
            description: str, optional
                Description of the monitoring configuration.

        Returns:
            FeatureMonitoringConfig The monitoring configuration.
        """
        return fmc.FeatureMonitoringConfig(
            feature_store_id=self._feature_store_id,
            feature_group_id=self._feature_group_id,
            feature_view_id=self._feature_view_id,
            feature_view_name=self._feature_view_name,
            feature_view_version=self._feature_view_version,
            feature_name=feature_name,
            name=name,
            description=description,
            feature_monitoring_type="SCHEDULED_STATISTICS",
            detection_window_config=detection_window_config,
            scheduler_config=scheduler_config,
            enabled=True,
            alert_config=None,
            reference_window_config=None,
            statistics_comparison_config=None,
        )

    def save(
        self, config: "fmc.FeatureMonitoringConfig"
    ) -> "fmc.FeatureMonitoringConfig":
        """Saves a feature monitoring config.

        Args:
            config: FeatureMonitoringConfig, required
                The feature monitoring config to save.

        Returns:
            FeatureMonitoringConfig The saved feature monitoring config.

        Raises:
            FeatureStoreException: If the config is already registered.
        """
        if config._id is not None:
            raise FeatureStoreException(
                "Cannot save a config that is already registered."
                " Please use update() instead."
            )
        return self._feature_monitoring_config_api.create(config)

    def update(
        self, config: "fmc.FeatureMonitoringConfig"
    ) -> "fmc.FeatureMonitoringConfig":
        """Updates a feature monitoring config.

        Args:
            config: FeatureMonitoringConfig, required
                The feature monitoring config to update.

        Returns:
            FeatureMonitoringConfig The updated feature monitoring config.

        Raises:
            FeatureStoreException: If the config is not registered.
        """
        if config._id is None:
            raise FeatureStoreException(
                "Cannot update a config that is not registered."
                " Please use save() instead."
            )
        return self._feature_monitoring_config_api.update(config)

    def pause_or_resume_monitoring(self, config_id: int, enabled: bool) -> None:
        """Enable or disable a feature monitoring config.

        Args:
            config_id: int, required
                The id of the feature monitoring config to enable.
            enabled: bool, required
                Whether to enable (true) or disable (true) the feature monitoring config.
        """
        self._feature_monitoring_config_api.pause_or_resume_monitoring(
            config_id=config_id,
            enabled=enabled,
        )

    def _build_feature_monitoring_config(
        self,
        feature_name: str,
        name: str,
        detection_window_config: MonitoringWindowConfig,
        reference_window_config: MonitoringWindowConfig,
        statistics_comparison_config: Dict[str, Any],
        scheduler_config: Optional[Union[JobScheduler, Dict[str, Any]]],
        alert_config: str,
        description: Optional[str] = None,
    ) -> "fmc.FeatureMonitoringConfig":
        """Builds a feature monitoring config.

        Args:
            feature_name: str, required
                Name of the feature to monitor.
            name: str, required
                Name of the monitoring configuration.
            detection_window_config: Dict[str, Any], required
                Configuration of the detection window.
            reference_window_config: Dict[str, Any], required
                Configuration of the reference window.
            statistics_comparison_config: Dict[str, Any], required
                Configuration of the statistics comparison.
            scheduler_config: Optional[Union[JobScheduler, Dict[str, Any]]], optional
                Configuration of the scheduler.
            alert_config: str, optional
                Configuration of the alert.
            description: str, optional
                Description of the monitoring configuration.
        """

        return fmc.FeatureMonitoringConfig(
            feature_store_id=self._feature_store_id,
            feature_group_id=self._feature_group_id,
            feature_view_id=self._feature_view_id,
            feature_view_name=self._feature_view_name,
            feature_view_version=self._feature_view_version,
            feature_name=feature_name,
            feature_monitoring_type="DESCRIPTIVE_STATISTICS",
            detection_window_config=detection_window_config,
            scheduler_config=scheduler_config,
            enabled=True,
            name=name,
            description=description,
            alert_config=alert_config,
            reference_window_config=reference_window_config,
            statistics_comparison_config=statistics_comparison_config,
        )

    def trigger_monitoring_job(
        self,
        job_name: str,
    ) -> Job:
        """Make a REST call to start an execution of the monitoring job.

        Args:
            job_name: Name of the job to trigger.

        Returns:
            Job object.
        """
        job_api = JobApi()
        job_api.launch(name=job_name)

        return job_api.get(name=job_name)

    def run_feature_monitoring(
        self, entity, config_name: str, result_engine: FeatureMonitoringResultEngine
    ) -> FeatureMonitoringResult:
        """Main function used by the job to actually perform the monitoring.

        Args:
            entity (Union[Featuregroup, FeatureView]): Featuregroup or Featureview
                object containing the feature to monitor.
            config_str: name of the monitoring config.
            result_engine: result engine to
                upload the outcome of the monitoring to the backend.

        Returns:
            FeatureMonitoringResult: A result object describing the
                outcome of the monitoring.
        """

        config = self._feature_monitoring_config_api.get_by_name(config_name)

        # TODO: Future work. Parallelize both single_window_monitoring calls and wait
        detection_stats = self.run_single_window_monitoring(
            entity=entity,
            monitoring_window_config=config.detection_window_config,
            feature_name=config.feature_name,
            check_existing=False,
        )

        if config.reference_window_config is not None:
            reference_stats = self.run_single_window_monitoring(
                entity=entity,
                monitoring_window_config=config.reference_window_config,
                feature_name=config.feature_name,
                check_existing=True,
            )
        else:
            reference_stats = None

        return result_engine.run_and_save_statistics_comparison(
            config,
            detection_stats,
            reference_stats,
        )

    def run_single_window_monitoring(
        self,
        entity,
        monitoring_window_config: MonitoringWindowConfig,
        feature_name: str,
        check_existing: bool = False,
    ) -> Union[FeatureDescriptiveStatistics, float]:
        """Fetch the entity data based on monitoring window configuration and compute statistics.

        Args:
            entity: FeatureStore: Feature store to fetch the entity to monitor.
            monitoring_window_config: MonitoringWindowConfig: Monitoring window config.
            feature_name: str: Name of the feature to monitor.
            check_existing: bool: Whether to check for existing stats.

        Returns:
            Union[FeatureDescriptiveStatitics, float]: Descriptive statistics or a specific value
        """

        if (
            monitoring_window_config.window_config_type
            == WindowConfigType.SPECIFIC_VALUE
        ):
            # if window config type is specific value, there is no stats to compute
            return monitoring_window_config.specific_value

        if check_existing:
            start_time, end_time = self.get_window_start_end_times(
                time_offset=monitoring_window_config.time_offset,
                window_length=monitoring_window_config.window_length,
            )
            registered_stats = self._statistics_engine.get_by_feature_name_time_window_and_row_percentage(
                entity,
                feature_name,
                start_time,
                end_time,
                monitoring_window_config.row_percentage,
            )
            if registered_stats is not None:
                return registered_stats

        # Fetch the actual data for which to compute statistics based on row_percentage and time window
        time_offset = self.time_range_str_to_time_delta(
            monitoring_window_config.time_offset
        )
        window_length = self.time_range_str_to_time_delta(
            monitoring_window_config.window_length
        )
        start_time, end_time = self.get_window_start_end_times(
            time_offset, window_length
        )
        entity_feature_df = (
            self.fetch_entity_data_based_on_time_window_and_row_percentage(
                entity=entity,
                feature_name=feature_name,
                start_time=start_time,
                end_time=end_time,
                row_percentage=monitoring_window_config.row_percentage,
            )
        )

        # Compute statistics on the feature dataframe
        descriptive_stats = self._statistics_engine.compute_single_feature_statistics(
            entity_feature_df,
            feature_name,
        )
        # set commit times and row percentage
        descriptive_stats.start_time = start_time
        descriptive_stats.end_time = end_time
        descriptive_stats.row_percentage = monitoring_window_config.row_percentage

        return descriptive_stats

    def fetch_entity_data_based_on_time_window_and_row_percentage(
        self,
        entity,  #: Union[feature_group.FeatureGroup, feature_view.FeatureView],
        feature_name: str,
        start_time: int,
        end_time: int,
        row_percentage: int = 100,
    ):
        """Fetch the entity data based on time window and row percentage.

        Args:
            entity: Union[FeatureGroup, FeatureView]: Entity to monitor.
            feature_name: str: Name of the feature to monitor.
            start_time: int: Window start commit time
            end_time: int: Window end commit time
            row_percentage: Percentage of rows to include

        Returns:
            `pyspark.DataFrame`. A Spark DataFrame with the entity data
        """
        if entity.ENTITY_TYPE == "featuregroups":
            full_df = (
                entity.select(features=[feature_name])
                .as_of(exclude_until=start_time, wallclock_time=end_time)
                .read()
            )
        elif entity.ENTITY_TYPE == "featureview":
            # TODO: This is a hack to get the data from the feature view without using get_batch_data
            # Currently get_batch_data does not support returning pandas dataframes
            # We also need to pass a training data version if there are transformation
            # functions to apply for get_batch_data to work
            full_df = (
                entity.query.as_of(exclude_until=start_time, wallclock_time=end_time)
                .read()
                .select(feature_name)
            )

        if row_percentage < 100:
            full_df = full_df.sample(fraction=(row_percentage / 100))

        return full_df

    def fetch_statistics_based_on_monitoring_window_config(
        self,
        entity,  #: Union[feature_group.FeatureGroup, feature_view.FeatureView],
        feature_name: str,
        monitoring_window_config: MonitoringWindowConfig,
    ) -> FeatureDescriptiveStatistics:
        """Fetch feature statistics based on a feature monitoring window configuration

        Args:
            entity: Union[FeatureGroup, FeatureView]: Entity on which statistics where computed.
            feature_name: str: Name of the feature from which statistics where computed.
            monitoring_window_config: MonitoringWindowConfig: Monitoring window config.

        Returns:
            FeatureDescriptiveStatistics: Descriptive statistics
        """
        start_time, end_time = self.get_window_start_end_times(
            time_offset=monitoring_window_config.time_offset,
            window_length=monitoring_window_config.window_length,
        )
        return (
            self._statistics_engine.get_by_feature_name_time_window_and_row_percentage(
                entity,
                feature_name,
                start_time,
                end_time,
                monitoring_window_config.row_percentage,
            )
        )

    def time_range_str_to_time_delta(self, time_range: str) -> timedelta:
        months, weeks, days, hours = re.search(
            r"(\d+)m(\d+)w(\d+)d(\d+)h",
            time_range,
        ).groups(0)

        return timedelta(months=months, weeks=weeks, days=days, hours=hours)

    def get_window_start_end_times(self, time_offset, window_length):
        return (
            datetime.now() - time_offset,
            datetime.now() - time_offset + window_length,
        )
