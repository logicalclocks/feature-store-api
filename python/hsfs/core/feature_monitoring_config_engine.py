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
from datetime import date, datetime
import re

from hsfs.core import feature_monitoring_config_api
from hsfs.core import feature_monitoring_config as fmc
from hsfs.core.feature_monitoring_result_engine import FeatureMonitoringResultEngine
from hsfs.core.statistics_engine import StatisticsEngine
from hsfs.core.feature_monitoring_result import FeatureMonitoringResult
from hsfs.core import monitoring_window_config as mwc
from hsfs.core import monitoring_window_config_engine

from hsfs.core.job import Job
from hsfs.core.job_api import JobApi
from hsfs.core.job_scheduler import JobScheduler
from hsfs.util import convert_event_time_to_timestamp
from hsfs.client.exceptions import FeatureStoreException

from hsfs import feature_group, feature_view


class FeatureMonitoringConfigEngine:
    def __init__(
        self,
        feature_store_id: int,
        feature_group_id: Optional[int] = None,
        feature_view_id: Optional[int] = None,
        feature_view_name: Optional[str] = None,
        feature_view_version: Optional[int] = None,
    ) -> "FeatureMonitoringConfigEngine":
        """Business logic for feature monitoring configuration.

        This class encapsulates the business logic for feature monitoring configuration.
        It is responsible for routing methods from the public python API to the
        appropriate REST calls. It should also contain validation and error handling logic
        for payloads or default object. Additionally, it contains logic necessary
        to run the feature monitoring job, including taking a monitoring window configuration
        and fetching the associated data.

        Attributes:
            feature_store_id: int. Id of the respective Feature Store.
            feature_group_id: int. Id of the feature group, if monitoring a feature group.
            feature_view_id: int. Id of the feature view, if monitoring a feature view.
            feature_view_name: str. Name of the feature view, if monitoring a feature view.
            feature_view_version: int. Version of the feature view, if monitoring a feature view.
        """
        self._feature_store_id = feature_store_id
        self._feature_group_id = feature_group_id
        self._feature_view_id = feature_view_id
        self._feature_view_name = feature_view_name
        self._feature_view_version = feature_view_version

        if feature_group_id is None:
            assert feature_view_id is not None
            assert feature_view_name is not None
            assert feature_view_version is not None
            entity_type = "featureview"
        else:
            entity_type = "featuregroups"

        self._feature_monitoring_config_api = (
            feature_monitoring_config_api.FeatureMonitoringConfigApi(
                feature_store_id=feature_store_id,
                feature_group_id=feature_group_id,
                feature_view_name=feature_view_name,
                feature_view_version=feature_view_version,
            )
        )
        self._statistics_engine = StatisticsEngine(feature_store_id, entity_type)
        self._job_api = JobApi()
        self._monitoring_window_config_engine = (
            monitoring_window_config_engine.MonitoringWindowConfigEngine()
        )
        self._result_engine = FeatureMonitoringResultEngine(
            feature_store_id=feature_store_id,
            feature_group_id=feature_group_id,
            feature_view_id=feature_view_id,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
        )

        # Should we promote the variables below to static?
        self._VALID_CATEGORICAL_METRICS = [
            "completeness",
            "num_records_non_null",
            "num_records_null",
            "distinctness",
            "entropy",
            "uniqueness",
            "approximate_num_distinct_values",
            "exact_num_distinct_values",
        ]
        self._VALID_FRACTIONAL_METRICS = [
            "completeness",
            "num_records_non_null",
            "num_records_null",
            "distinctness",
            "entropy",
            "uniqueness",
            "approximate_num_distinct_values",
            "exact_num_distinct_values",
            "mean",
            "maximum",
            "minimum",
            "sum",
            "std_dev",
            "count",
        ]

    def enable_descriptive_statistics_monitoring(
        self,
        name: str,
        feature_name: str,
        detection_window_config: "mwc.MonitoringWindowConfig",
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
        detection_window_config: "mwc.MonitoringWindowConfig",
        reference_window_config: "mwc.MonitoringWindowConfig",
        statistics_comparison_config: Dict[str, Any],
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
            description=description,
        )

        return self._feature_monitoring_config_api.create(
            fm_config=config,
        )

    def validate_statistics_comparison_config(
        self,
        metric: str,
        threshold: Union[float, int],
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
        if not isinstance(relative, bool):
            raise ValueError("relative must be a boolean value.")

        if not isinstance(strict, bool):
            raise ValueError("strict must be a boolean value.")

        if not isinstance(threshold, (int, float)):
            raise TypeError("threshold must be a numeric value.")

        if not isinstance(metric, str):
            raise TypeError(
                "metric must be a string value. "
                "Check the documentation for a list of supported metrics."
            )
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

    def _build_default_statistics_monitoring_config(
        self,
        name: str,
        feature_name: Optional[str] = None,
        job_frequency: Optional[str] = "DAILY",
        start_date_time: Optional[Union[str, int, date, datetime]] = None,
        description: Optional[str] = None,
        valid_feature_names: Optional[List[str]] = None,
        use_event_time: Optional[bool] = False,
        transformation_function_dataset_version: Optional[int] = None,
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
            use_event_time: bool, optional
                If true, use event time to compute statistics. Feature View only.
                Defaults to False.
            transformation_function_dataset_version: int, optional
                The version of the dataset to use to fetch statistics for the
                transformation function. Feature View only. If provided, the
                statistics are computed after applying the transformation function.
                Defaults to None.

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
            feature_monitoring_type=fmc.FeatureMonitoringType.STATISTICS_MONITORING,
            scheduler_config={
                "job_frequency": job_frequency,
                "start_date_time": start_date_time,
                "enabled": True,
            },
            use_event_time=use_event_time,
            transformation_function_dataset_version=transformation_function_dataset_version,
        ).with_detection_window()

    def _build_default_feature_monitoring_config(
        self,
        name: str,
        feature_name: str,
        job_frequency: Optional[str] = "DAILY",
        start_date_time: Optional[Union[str, int, date, datetime]] = None,
        description: Optional[str] = None,
        valid_feature_names: Optional[List[str]] = None,
        use_event_time: Optional[bool] = False,
        transformation_function_dataset_version: Optional[int] = None,
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
            use_event_time: bool, optional
                If true, use event time to compute statistics. Feature View only.
                Defaults to False.
            transformation_function_dataset_version: int, optional
                The version of the dataset to use to fetch statistics for the
                transformation function. Feature View only. If provided, the
                statistics are computed after applyin the transformation function.

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
            # setting feature_monitoring_type to "STATISTICS_COMPARISON" allows
            # to raise an error if no reference window and comparison config are provided
            feature_monitoring_type=fmc.FeatureMonitoringType.STATISTICS_COMPARISON,
            scheduler_config={
                "job_frequency": job_frequency,
                "start_date_time": start_date_time,
                "enabled": True,
            },
            use_event_time=use_event_time,
            transformation_function_dataset_version=transformation_function_dataset_version,
        ).with_detection_window()  # TODO: Do we want to have a default reference window + stat comparison?

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

    def delete(self, config_id: int) -> None:
        """Deletes a feature monitoring config.

        Args:
            config_id: int, required
                The id of the feature monitoring config to delete.
        """
        self._feature_monitoring_config_api.delete(config_id=config_id)

    def get_feature_monitoring_configs(
        self,
        name: Optional[str] = None,
        feature_name: Optional[str] = None,
        config_id: Optional[int] = None,
    ) -> Union[
        "fmc.FeatureMonitoringConfig", List["fmc.FeatureMonitoringConfig"], None
    ]:
        """Fetch feature monitoring configuration by entity, name or feature name.

        If no arguments are provided, fetches all feature monitoring configurations
        attached to the given entity. If a name is provided, it fetches a single configuration
        and returns None if not found. If a feature name is provided, it fetches all
        configurations attached to that feature (not including those attached to the full
        entity) and returns an empty list if none are found. If a config_id is provided,
        it fetches a single configuration and returns None if not found.

        Args:
            name: str, optional
                If provided, fetch only configuration with given name.
                Defaults to None.
            feature_name: str, optional
                If provided, fetch all configurations attached to a specific feature.
                Defaults to None.
            config_id: int, optional
                If provided, fetch only configuration with given id.
                Defaults to None.

        Raises:
            ValueError: If both name and feature_name are provided.
            TypeError: If name or feature_name are not strings.

        Returns:
            FeatureMonitoringConfig or List[FeatureMonitoringConfig] The monitoring
            configuration(s).
        """
        if any(
            [
                name and feature_name,
                feature_name and config_id,
                config_id and name,
            ]
        ):
            raise ValueError("Provide at most one of name, feature_name, or config_id.")

        if name is not None:
            if not isinstance(name, str):
                raise TypeError("name must be a string or None.")
            return self._feature_monitoring_config_api.get_by_name(name=name)
        elif feature_name is not None:
            if not isinstance(feature_name, str):
                raise TypeError("feature_name must be a string or None.")
            return self._feature_monitoring_config_api.get_by_feature_name(
                feature_name=feature_name
            )
        elif config_id is not None:
            if not isinstance(config_id, int):
                raise TypeError("config_id must be an integer or None.")
            return self._feature_monitoring_config_api.get_by_id(config_id=config_id)

        return self._feature_monitoring_config_api.get_by_entity()

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
            feature_monitoring_type=fmc.FeatureMonitoringType.STATISTICS_MONITORING,
            detection_window_config=detection_window_config,
            scheduler_config=scheduler_config,
            enabled=True,
            reference_window_config=None,
            statistics_comparison_config=None,
        )

    def _build_feature_monitoring_config(
        self,
        feature_name: str,
        name: str,
        detection_window_config: "mwc.MonitoringWindowConfig",
        reference_window_config: "mwc.MonitoringWindowConfig",
        statistics_comparison_config: Dict[str, Any],
        scheduler_config: Optional[Union[JobScheduler, Dict[str, Any]]],
        description: Optional[str] = None,
        use_event_time: bool = False,
        transformation_function_dataset_version=None,
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
            description: str, optional
                Description of the monitoring configuration.
            use_event_time: bool, optional
                Whether to use event time or ingestion time. Defaults to False.
            transformation_function_dataset_version: str, optional
                The version of the dataset to use to fetch statistics for the
                transformation function. If None, no transformation functions are
                applied to the statistics. Defaults to None.
        """

        return fmc.FeatureMonitoringConfig(
            feature_store_id=self._feature_store_id,
            feature_group_id=self._feature_group_id,
            feature_view_id=self._feature_view_id,
            feature_view_name=self._feature_view_name,
            feature_view_version=self._feature_view_version,
            feature_name=feature_name,
            feature_monitoring_type=fmc.FeatureMonitoringType.STATISTICS_COMPARISON,
            detection_window_config=detection_window_config,
            scheduler_config=scheduler_config,
            enabled=True,
            name=name,
            description=description,
            reference_window_config=reference_window_config,
            statistics_comparison_config=statistics_comparison_config,
            use_event_time=use_event_time,
            transformation_function_dataset_version=transformation_function_dataset_version,
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
        self._job_api.launch(name=job_name)

        return self._job_api.get(name=job_name)

    def get_monitoring_job(
        self,
        job_name: str,
    ) -> Job:
        """Make a REST call to fetch the job entity.

        Args:
            job_name: Name of the job to trigger.

        Returns:
            `Job` A Hopsworks job with its metadata and execution history.
        """
        return self._job_api.get(name=job_name)

    def run_feature_monitoring(
        self,
        entity: Union["feature_group.FeatureGroup", "feature_view.FeatureView"],
        config_name: str,
    ) -> List[FeatureMonitoringResult]:
        """Main function used by the job to actually perform the monitoring.

        Args:
            entity: Union["feature_group.FeatureGroup", "feature_view.FeatureView"]
                Featuregroup or Featureview object containing the feature to monitor.
            config_name: str: name of the monitoring config.

        Returns:
            List[FeatureMonitoringResult]: A list of result object describing the
                outcome of the monitoring.
        """
        config = self._feature_monitoring_config_api.get_by_name(config_name)

        # TODO: Future work. Parallelize both single_window_monitoring calls and wait
        detection_statistics = (
            self._monitoring_window_config_engine.run_single_window_monitoring(
                entity=entity,
                monitoring_window_config=config.detection_window_config,
                feature_name=config.feature_name,
            )
        )

        if config.reference_window_config is not None:
            if (
                config.reference_window_config.window_config_type
                == mwc.WindowConfigType.SPECIFIC_VALUE
            ):
                specific_value = config.reference_window_config.specific_value
                reference_statistics = None
            else:
                reference_statistics = (
                    self._monitoring_window_config_engine.run_single_window_monitoring(
                        entity=entity,
                        monitoring_window_config=config.reference_window_config,
                        feature_name=config.feature_name,
                    )
                )
                specific_value = None
        else:
            specific_value, reference_statistics = None, None

        return self._result_engine.run_and_save_statistics_comparison(
            fm_config=config,
            detection_statistics=detection_statistics,
            reference_statistics=reference_statistics,
            specific_value=specific_value,
        )
