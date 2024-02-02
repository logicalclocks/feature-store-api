#
#   Copyright 2024 Hopsworks AB
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
import humps
from enum import Enum
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Union
from hsfs.util import FeatureStoreEncoder
from hsfs.client.exceptions import FeatureStoreException

from hsfs.core import monitoring_window_config as mwc
from hsfs.core.feature_monitoring_result import FeatureMonitoringResult
from hsfs.core.job_schedule import JobSchedule
from hsfs.core import (
    monitoring_window_config_engine,
    feature_monitoring_config_engine,
    feature_monitoring_result_engine,
)
from hsfs.core import job_api

MAX_LENGTH_NAME = 63
MAX_LENGTH_DESCRIPTION = 2000


class FeatureMonitoringType(str, Enum):
    STATISTICS_COMPUTATION = "STATISTICS_COMPUTATION"  # only stats computation
    STATISTICS_COMPARISON = "STATISTICS_COMPARISON"  # stats computation and comparison
    PROBABILITY_DENSITY_FUNCTION = "PROBABILITY_DENSITY_FUNCTION"  # data distributions

    @classmethod
    def list_str(cls) -> List[str]:
        return list(map(lambda c: c.value, cls))

    @classmethod
    def list(cls) -> List["FeatureMonitoringType"]:
        return list(map(lambda c: c, cls))

    @classmethod
    def from_str(cls, value: str) -> "FeatureMonitoringType":
        if value in cls.list_str():
            return cls(value)
        else:
            raise ValueError(
                f"Invalid value {value} for FeatureMonitoringType, allowed values are {cls.list_str()}"
            )

    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        if isinstance(other, self.__class__):
            return self is other
        return False

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return self.value


class FeatureMonitoringConfig:
    def __init__(
        self,
        feature_store_id: int,
        name: str,
        feature_name: Optional[str] = None,
        feature_monitoring_type: Union[
            FeatureMonitoringType, str
        ] = FeatureMonitoringType.STATISTICS_COMPUTATION,
        job_name: Optional[str] = None,
        detection_window_config: Optional[
            Union[mwc.MonitoringWindowConfig, Dict[str, Any]]
        ] = None,
        reference_window_config: Optional[
            Union[mwc.MonitoringWindowConfig, Dict[str, Any]]
        ] = None,
        statistics_comparison_config: Optional[Dict[str, Any]] = None,
        job_schedule: Optional[Union[Dict[str, Any], JobSchedule]] = None,
        description: Optional[str] = None,
        id: Optional[int] = None,
        feature_group_id: Optional[int] = None,
        feature_view_name: Optional[str] = None,
        feature_view_version: Optional[int] = None,
        href: Optional[str] = None,
        **kwargs,
    ):
        self.name = name
        self._id = id
        self._href = href
        self.description = description
        self._feature_name = feature_name
        self._feature_store_id = feature_store_id
        self._feature_group_id = feature_group_id
        self._feature_view_name = feature_view_name
        self._feature_view_version = feature_view_version
        self._job_name = job_name
        self._feature_monitoring_type = FeatureMonitoringType(feature_monitoring_type)

        self._feature_monitoring_config_engine = (
            feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
                feature_store_id=feature_store_id,
                feature_group_id=feature_group_id,
                feature_view_name=feature_view_name,
                feature_view_version=feature_view_version,
            )
        )
        self._monitoring_window_config_engine = (
            monitoring_window_config_engine.MonitoringWindowConfigEngine()
        )
        self._feature_monitoring_result_engine = (
            feature_monitoring_result_engine.FeatureMonitoringResultEngine(
                feature_store_id=feature_store_id,
                feature_group_id=feature_group_id,
                feature_view_name=feature_view_name,
                feature_view_version=feature_view_version,
            )
        )
        self._job_api = job_api.JobApi()

        self.detection_window_config = detection_window_config
        self.reference_window_config = reference_window_config
        self.statistics_comparison_config = statistics_comparison_config
        self.job_schedule = job_schedule

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [cls(**config) for config in json_decamelized["items"]]
        else:
            return cls(**json_decamelized)

    def to_dict(self):
        detection_window_config = (
            self._detection_window_config.to_dict()
            if self._detection_window_config is not None
            else None
        )
        reference_window_config = (
            self._reference_window_config.to_dict()
            if self._reference_window_config is not None
            else None
        )
        if isinstance(self._statistics_comparison_config, dict):
            statistics_comparison_config = {
                "id": self._statistics_comparison_config.get("id", None),
                "threshold": self._statistics_comparison_config.get("threshold", 0.0),
                "metric": self._statistics_comparison_config.get("metric", "MEAN"),
                "strict": self._statistics_comparison_config.get("strict", False),
                "relative": self._statistics_comparison_config.get("relative", False),
            }
        else:
            statistics_comparison_config = None
        if isinstance(self._job_schedule, JobSchedule):
            job_schedule = self._job_schedule.to_dict()
        else:
            job_schedule = None

        the_dict = {
            "id": self._id,
            "featureStoreId": self._feature_store_id,
            "featureName": self._feature_name,
            "name": self._name,
            "description": self._description,
            "jobName": self._job_name,
            "featureMonitoringType": self._feature_monitoring_type,
            "jobSchedule": job_schedule,
            "detectionWindowConfig": detection_window_config,
        }

        if self._feature_group_id is not None:
            the_dict["featureGroupId"] = self._feature_group_id
        elif (
            self._feature_view_name is not None
            and self._feature_view_version is not None
        ):
            the_dict["featureViewName"] = self._feature_view_name
            the_dict["featureViewVersion"] = self._feature_view_version

        if (
            self._feature_monitoring_type
            == FeatureMonitoringType.STATISTICS_COMPUTATION
        ):
            return the_dict

        the_dict["referenceWindowConfig"] = reference_window_config
        the_dict["statisticsComparisonConfig"] = statistics_comparison_config

        return the_dict

    def json(self) -> str:
        return json.dumps(self, cls=FeatureStoreEncoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"FeatureMonitoringConfig({self._name!r}, {self._feature_monitoring_type!r})"

    def with_detection_window(
        self,
        time_offset: Optional[str] = None,
        window_length: Optional[str] = None,
        row_percentage: Optional[float] = None,
    ) -> "FeatureMonitoringConfig":
        """Sets the detection window for the feature monitoring job.
        !!! example
            ```python
            # fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)
            # Compute statistics on a regular basis
            fg.create_statistics_monitoring(
                name="regular_stats",
                cron_expression="0 0 12 ? * * *",
            ).with_detection_window(
                time_offset="1d",
                window_length="1d",
                row_percentage=0.1,
            ).save()
            # Compute and compare statistics
            fg.create_feature_monitoring(
                name="regular_stats",
                feature_name="my_feature",
                cron_expression="0 0 12 ? * * *",
            ).with_detection_window(
                time_offset="1d",
                window_length="1d",
                row_percentage=0.1,
            ).with_reference_window(...).compare_on(...).save()
            ```
        # Arguments
            time_offset: The time offset from the current time to the start of the time window.
            window_length: The length of the time window.
            row_percentage: The fraction of rows to use when computing the statistics [0, 1.0].
        # Returns
            `FeatureMonitoringConfig`. The updated FeatureMonitoringConfig object.
        """
        # Setter is using the engine class to perform input validation.
        self.detection_window_config = {
            "window_config_type": (
                mwc.WindowConfigType.ROLLING_TIME
                if time_offset or window_length
                else mwc.WindowConfigType.ALL_TIME
            ),
            "time_offset": time_offset,
            "window_length": window_length,
            "row_percentage": row_percentage,
        }

        return self

    def with_reference_window(
        self,
        time_offset: Optional[str] = None,
        window_length: Optional[str] = None,
        row_percentage: Optional[float] = None,
    ) -> "FeatureMonitoringConfig":
        """Sets the reference window for the feature monitoring job.
        See also `with_reference_value(...)` and `with_reference_training_dataset(...)` for other reference options.
        !!! example
            ```python
            # fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)
            # Setup feature monitoring and a detection window
            my_monitoring_config = fg.create_feature_monitoring(...).with_detection_window(...)
            # Statistics computed on a rolling time window, e.g. same day last week
            my_monitoring_config.with_reference_window(
                time_offset="1w",
                window_length="1d",
            ).compare_on(...).save()
            ```
        !!! note
            You must provide a comparison configuration via compare_on(...) before saving the feature monitoring config.
        # Arguments
            time_offset: The time offset from the current time to the start of the time window.
            window_length: The length of the time window.
            row_percentage: The percentage of rows to use when computing the statistics. Defaults to 20%.
        # Returns
            `FeatureMonitoringConfig`. The updated FeatureMonitoringConfig object.
        """
        # Setter is using the engine class to perform input validation and build monitoring window config object.
        if self.detection_window_config is None:
            self.detection_window_config = {
                "window_config_type": mwc.WindowConfigType.ALL_TIME,
                "row_percentage": 1.0,
            }
        self.reference_window_config = {
            "time_offset": time_offset,
            "window_length": window_length,
            "row_percentage": row_percentage,
        }

        return self

    def with_reference_value(
        self,
        value: Optional[Union[float, int]] = None,
    ) -> "FeatureMonitoringConfig":
        """Sets the reference value for the feature monitoring job.
        See also `with_reference_window(...)` and `with_reference_training_dataset(...)` for other reference options.
        !!! example
            ```python
            # fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)
            # Setup feature monitoring and a detection window
            my_monitoring_config = fg.create_feature_monitoring(...).with_detection_window(...)
            # Simplest reference window is a specific value
            my_monitoring_config.with_reference_value(
                value=0.0,
            ).compare_on(...).save()
            ```
        !!! note
            You must provide a comparison configuration via compare_on(...) before saving the feature monitoring config.
        # Arguments
            value: A float value to use as reference.
        # Returns
            `FeatureMonitoringConfig`. The updated FeatureMonitoringConfig object.
        """
        self.reference_window_config = {
            "specific_value": value,
        }

        return self

    def with_reference_training_dataset(
        self,
        training_dataset_version: Optional[int] = None,
    ) -> "FeatureMonitoringConfig":
        """Sets the reference training dataset for the feature monitoring job.
        See also `with_reference_value(...)` and `with_reference_window(...)` for other reference options.
        !!! example
            ```python
            # fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)
            # Setup feature monitoring and a detection window
            my_monitoring_config = fg.create_feature_monitoring(...).with_detection_window(...)
            # Only for feature views: Compare to the statistics computed for one of your training datasets
            # particularly useful if it has been used to train a model currently in production
            my_monitoring_config.with_reference_training_dataset(
                training_dataset_version=3,
            ).compare_on(...).save()
            ```
        !!! note
            You must provide a comparison configuration via compare_on(...) before saving the feature monitoring config.
        # Arguments
            training_dataset_version: The version of the training dataset to use as reference.
        # Returns
            `FeatureMonitoringConfig`. The updated FeatureMonitoringConfig object.
        """
        self.reference_window_config = {
            "training_dataset_version": training_dataset_version,
        }

        return self

    def compare_on(
        self,
        metric: Optional[str],
        threshold: Optional[float],
        strict: Optional[bool] = False,
        relative: Optional[bool] = False,
    ) -> "FeatureMonitoringConfig":
        """Sets the comparison configuration for monitoring involving a reference window.
        !!! example
            ```python
            # fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)
            # Setup feature monitoring, a detection window and a reference window
            my_monitoring_config = fg.create_feature_monitoring(
                ...
            ).with_detection_window(...).with_reference_window(...)
            # Choose a metric and set a threshold for the difference
            # e.g compare the relative mean of detection and reference window
            my_monitoring_config.compare_on(
                metric="mean",
                threshold=1.0,
                relative=True,
            ).save()
            ```
        !!! note
            Detection window and reference window/value/training_dataset must be set prior to comparison configuration.
        # Arguments
            metric: The metric to use for comparison. Different metric are available for different feature type.
            threshold: The threshold to apply to the difference to potentially trigger an alert.
            strict: Whether to use a strict comparison (e.g. > or <) or a non-strict comparison (e.g. >= or <=).
            relative: Whether to use a relative comparison (e.g. relative mean) or an absolute comparison (e.g. absolute mean).
        # Returns
            `FeatureMonitoringConfig`. The updated FeatureMonitoringConfig object.
        """
        # Setter is using the engine class to perform input validation.
        self.statistics_comparison_config = {
            "metric": metric,
            "threshold": threshold,
            "strict": strict,
            "relative": relative,
        }

        return self

    def save(self):
        """Saves the feature monitoring configuration to the backend.
        !!! example
            ```python
            # fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)
            # Setup feature monitoring and a detection window
            my_monitoring_config = fg.create_statistics_monitoring(
                name="my_monitoring_config",
            ).save()
            ```
        # Returns
            `FeatureMonitoringConfig`. The saved FeatureMonitoringConfig object.
        """
        registered_config = self._feature_monitoring_config_engine.save(self)
        self.detection_window_config = registered_config._detection_window_config
        self.job_schedule = registered_config._job_schedule

        if (
            self._feature_monitoring_type
            != FeatureMonitoringType.STATISTICS_COMPUTATION
        ):
            self.reference_window_config = registered_config._reference_window_config
            self.statistics_comparison_config = (
                registered_config._statistics_comparison_config
            )

        self._job_name = registered_config._job_name
        self._id = registered_config._id
        return self

    def update(self):
        """Updates allowed fields of the saved feature monitoring configuration.
        !!! example
            ```python
            # fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)
            # fetch registered config by name
            my_monitoring_config = fg.get_feature_monitoring_config(name="my_monitoring_config")
            # update the percentage of rows to use when computing the statistics
            my_monitoring_config.detection_window.row_percentage = 10
            my_monitoring_config.update()
            ```
        # Returns
            `FeatureMonitoringConfig`. The updated FeatureMonitoringConfig object.
        """
        return self._feature_monitoring_config_engine.update(self)

    def run_job(self):
        """Trigger the monitoring job which computes statistics on detection and reference window.
        !!! example
            ```python3
            # fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)
            # fetch registered config by name
            my_monitoring_config = fg.get_feature_monitoring_configs(name="my_monitoring_config")
            # trigger the job which computes statistics on detection and reference window
            my_monitoring_config.run_job()
            ```
        !!! info
            The job will be triggered asynchronously and the method will return immediately.
        # Raises
            `FeatureStoreException`: If the feature monitoring config has not been saved.
        # Returns
            `Job`. A handle for the job computing the statistics.
        """
        if not self._id or not self._job_name:
            raise FeatureStoreException(
                "Feature monitoring config must be registered via `.save()` before computing statistics."
            )

        return self._feature_monitoring_config_engine.trigger_monitoring_job(
            job_name=self.job_name
        )

    def get_job(self):
        """Get the monitoring job which computes statistics on detection and reference window.
        !!! example
            ```python3
            # fetch registered config by name via feature group or feature view
            my_monitoring_config = fg.get_feature_monitoring_configs(name="my_monitoring_config")
            # get the job which computes statistics on detection and reference window
            job = my_monitoring_config.get_job()
            # print job history and ongoing executions
            job.executions
            ```
        # Raises
            `FeatureStoreException`: If the feature monitoring config has not been saved.
        # Returns
            `Job`. A handle for the job computing the statistics.
        """
        if not self._id or not self._job_name:
            raise FeatureStoreException(
                "Feature monitoring config must be registered via `.save()` before fetching"
                "the associated job."
            )

        return self._feature_monitoring_config_engine.get_monitoring_job(
            job_name=self.job_name
        )

    def delete(self):
        """Deletes the feature monitoring configuration from the backend.
        !!! example
            ```python
            # fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)
            # fetch registered config by name
            my_monitoring_config = fg.get_feature_monitoring_config(name="my_monitoring_config")
            # delete the feature monitoring config
            my_monitoring_config.delete()
            ```
        # Raises
            `FeatureStoreException`: If the feature monitoring config has not been saved.
        """
        if not self._id:
            raise FeatureStoreException(
                "Feature monitoring config must be registered via `.save()` before deleting."
            )

        self._feature_monitoring_config_engine.delete(config_id=self._id)

    def disable(self):
        """Disables the spawning of monitoring job at time-interval controlled by the scheduler.
        !!! example
            ```python
            # fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)
            # fetch registered config by name
            my_monitoring_config = fg.get_feature_monitoring_config(name="my_monitoring_config")
            # disable the feature monitoring config
            my_monitoring_config.disable()
            ```
        # Raises
            `FeatureStoreException`: If the feature monitoring config has not been saved.
        """
        self._update_schedule(enabled=False)

    def enable(self):
        """Enables the spawning of monitoring job at time-interval controlled by the scheduler.
        The scheduler can be configured via the `job_schedule` property.
        !!! example
            ```python
            # fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)
            # fetch registered config by name
            my_monitoring_config = fg.get_feature_monitoring_config(name="my_monitoring_config")
            # enable the feature monitoring config
            my_monitoring_config.enable()
            ```
        # Raises
            `FeatureStoreException`: If the feature monitoring config has not been saved.
        """
        self._update_schedule(enabled=True)

    def _update_schedule(self, enabled):
        if self._job_schedule is None:
            raise FeatureStoreException("No scheduler found for monitoring job")
        job_schedule = JobSchedule(
            id=self._job_schedule.id,
            start_date_time=self._job_schedule.start_date_time,
            cron_expression=self._job_schedule.cron_expression,
            end_time=self._job_schedule.end_date_time,
            enabled=enabled,
        )
        self.job_schedule = self._job_api.create_or_update_schedule_job(
            self._job_name, job_schedule.to_dict()
        )
        return self._job_schedule

    def get_history(
        self,
        start_time: Union[datetime, date, str, int, None] = None,
        end_time: Union[datetime, date, str, int, None] = None,
        with_statistics: bool = True,
    ) -> List["FeatureMonitoringResult"]:
        """
        Fetch the history of the computed statistics for this configuration.
        !!! example
            ```python3
            # fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)
            # fetch registered config by name
            my_monitoring_config = fg.get_feature_monitoring_config(name="my_monitoring_config")
            # fetch the history of the computed statistics for this configuration
            history = my_monitoring_config.get_history(
                start_time="2021-01-01",
                end_time="2021-01-31",
            )
            ```
        # Args:
            start_time: The start time of the time range to fetch the history for.
            end_time: The end time of the time range to fetch the history for.
            with_statistics: Whether to include the computed statistics in the result.
        # Raises
            `FeatureStoreException`: If the feature monitoring config has not been saved.
        """
        if not self._id:
            raise FeatureStoreException(
                "Feature monitoring config must be registered via `.save()` before fetching"
                "the associated history."
            )
        return self._feature_monitoring_result_engine.fetch_all_feature_monitoring_results_by_config_id(
            config_id=self._id,
            start_time=start_time,
            end_time=end_time,
            with_statistics=with_statistics,
        )

    @property
    def id(self) -> Optional[int]:
        return self._id

    @property
    def feature_store_id(self) -> int:
        return self._feature_store_id

    @property
    def feature_group_id(self) -> Optional[int]:
        return self._feature_group_id

    @property
    def feature_name(self) -> Optional[str]:
        """The name of the feature to monitor. If not set, all features of the
        feature group or feature view are monitored, only available for statistics monitoring.
        !!! note
            This property is read-only after the feature monitoring config has been saved.
        """
        return self._feature_name

    @property
    def name(self) -> str:
        """The name of the feature monitoring config.
        A Feature Group or Feature View cannot have multiple feature monitoring configs with the same name. The name of
        a feature monitoring config is limited to 63 characters.
        !!! note
            This property is read-only after the feature monitoring config has been saved.
        """
        return self._name

    @name.setter
    def name(self, name: str):
        if hasattr(self, "_id"):
            raise AttributeError("The name of a registered config is read-only.")
        elif not isinstance(name, str):
            raise TypeError("name must be of type str")
        if len(name) > MAX_LENGTH_NAME:
            raise ValueError("name must be less than {MAX_LENGTH_NAME} characters.")
        self._name = name

    @property
    def description(self) -> Optional[str]:
        return self._description

    @description.setter
    def description(self, description: Optional[str]):
        if not isinstance(description, str) and description is not None:
            raise TypeError("description must be of type str")
        elif isinstance(description, str) and len(description) > MAX_LENGTH_DESCRIPTION:
            raise ValueError(
                "description must be less than {MAX_LENGTH_DESCRIPTION} characters"
            )
        self._description = description

    @property
    def job_name(self) -> Optional[str]:
        return self._job_name

    @property
    def enabled(self) -> bool:
        """Controls whether or not this config is spawning new monitoring jobs.
        This field belongs to the scheduler configuration but is made transparent to the user for convenience.
        """
        return self.job_schedule.enabled

    @enabled.setter
    def enabled(self, enabled: bool):
        """
        Controls whether or not this config is spawning new monitoring jobs.
        This field belongs to the scheduler configuration but is made transparent to the user for convenience.
        """
        self.job_schedule.enabled = enabled

    @property
    def feature_monitoring_type(self) -> Optional[str]:
        """The type of feature monitoring to perform. Used for internal validation.
        Options are:
            - STATISTICS_COMPUTATION if no reference window (and therefore comparison config) is provided
            - STATISTICS_COMPARISON if a reference window (and therefore comparison config) is provided.
        !!! note
            This property is read-only.
        """
        return self._feature_monitoring_type

    @property
    def job_schedule(self) -> JobSchedule:
        return self._job_schedule

    @job_schedule.setter
    def job_schedule(self, job_schedule: Union[JobSchedule, Dict[str, Any]]):
        if isinstance(job_schedule, JobSchedule):
            self._job_schedule = job_schedule
        elif isinstance(job_schedule, dict):
            self._job_schedule = JobSchedule(**job_schedule)
        else:
            raise TypeError("job_schedule must be of type JobScheduler, dict or None")

    @property
    def detection_window_config(self) -> mwc.MonitoringWindowConfig:
        return self._detection_window_config

    @detection_window_config.setter
    def detection_window_config(
        self,
        detection_window_config: Optional[
            Union[mwc.MonitoringWindowConfig, Dict[str, Any]]
        ],
    ):
        if isinstance(detection_window_config, mwc.MonitoringWindowConfig):
            self._detection_window_config = detection_window_config
        elif isinstance(detection_window_config, dict):
            self._detection_window_config = (
                self._monitoring_window_config_engine.build_monitoring_window_config(
                    **detection_window_config
                )
            )
        elif detection_window_config is None:
            self._detection_window_config = detection_window_config
        else:
            raise TypeError(
                "detection_window_config must be of type MonitoringWindowConfig, dict or None"
            )

    @property
    def reference_window_config(self) -> mwc.MonitoringWindowConfig:
        return self._reference_window_config

    @reference_window_config.setter
    def reference_window_config(
        self,
        reference_window_config: Optional[
            Union[mwc.MonitoringWindowConfig, Dict[str, Any]]
        ] = None,
    ):
        """Sets the reference window for monitoring."""
        # TODO: improve setter documentation
        if (
            self._feature_monitoring_type
            == FeatureMonitoringType.STATISTICS_COMPUTATION
            and reference_window_config is not None
        ):
            raise AttributeError(
                "reference_window_config is only available for feature monitoring"
                " not for scheduled statistics."
            )
        if isinstance(reference_window_config, mwc.MonitoringWindowConfig):
            self._reference_window_config = reference_window_config
        elif isinstance(reference_window_config, dict):
            self._reference_window_config = (
                self._monitoring_window_config_engine.build_monitoring_window_config(
                    **reference_window_config
                )
            )
        elif reference_window_config is None:
            self._reference_window_config = None
        else:
            raise TypeError(
                "reference_window_config must be of type MonitoringWindowConfig, dict or None"
            )

    @property
    def statistics_comparison_config(
        self,
    ) -> Optional[Dict[str, Any]]:
        return self._statistics_comparison_config

    @statistics_comparison_config.setter
    def statistics_comparison_config(
        self,
        statistics_comparison_config: Optional[Dict[str, Any]] = None,
    ):
        if (
            self._feature_monitoring_type
            == FeatureMonitoringType.STATISTICS_COMPUTATION
            and statistics_comparison_config is not None
        ):
            raise AttributeError(
                "statistics_comparison_config is only available for feature monitoring"
                " not for scheduled statistics."
            )
        if isinstance(statistics_comparison_config, dict):
            self._statistics_comparison_config = self._feature_monitoring_config_engine.validate_statistics_comparison_config(
                **statistics_comparison_config
            )
        elif statistics_comparison_config is None:
            self._statistics_comparison_config = statistics_comparison_config
        else:
            raise TypeError("statistics_comparison_config must be of type dict or None")
