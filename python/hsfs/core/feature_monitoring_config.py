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

import json
from hsfs.core.job_scheduler import JobScheduler
import humps
from typing import Any, Dict, Optional, Union
from hsfs.util import FeatureStoreEncoder
from hsfs.client.exceptions import FeatureStoreException

from hsfs.core.monitoring_window_config import MonitoringWindowConfig
from hsfs.core import feature_monitoring_config_engine


class FeatureMonitoringConfig:
    def __init__(
        self,
        feature_store_id: int,
        name: str,
        feature_name: Optional[str] = None,
        feature_monitoring_type: str = "DESCRIPTIVE_STATISTICS",
        job_name: Optional[str] = None,
        detection_window_config: Optional[
            Union[MonitoringWindowConfig, Dict[str, Any]]
        ] = None,
        reference_window_config: Optional[
            Union[MonitoringWindowConfig, Dict[str, Any]]
        ] = None,
        statistics_comparison_config: Optional[Dict[str, Any]] = None,
        alert_config: Optional[str] = None,
        scheduler_config: Optional[Union[Dict[str, Any], JobScheduler]] = None,
        description: Optional[str] = None,
        enabled: bool = True,
        id: Optional[int] = None,
        feature_group_id: Optional[int] = None,
        feature_view_id: Optional[int] = None,
        feature_view_name: Optional[str] = None,
        feature_view_version: Optional[int] = None,
        href: Optional[str] = None,
    ):
        self._id = id
        self._href = href
        # TODO: Validate name and description string length and character set
        # in setter to avoid issues with the backend
        self._name = name
        self._description = description
        self._feature_name = feature_name
        self._feature_store_id = feature_store_id
        self._feature_group_id = feature_group_id
        self._feature_view_id = feature_view_id
        self._job_name = job_name
        self._feature_monitoring_type = feature_monitoring_type
        self._enabled = enabled

        self._feature_monitoring_config_engine = (
            feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
                feature_store_id=feature_store_id,
                feature_group_id=feature_group_id,
                feature_view_id=feature_view_id,
                feature_view_name=feature_view_name,
                feature_view_version=feature_view_version,
            )
        )

        self.detection_window_config = detection_window_config
        self.reference_window_config = reference_window_config
        self.statistics_comparison_config = statistics_comparison_config
        self.scheduler_config = scheduler_config
        self._alert_config = alert_config

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [cls(**config) for config in json_decamelized["items"]]
        else:
            return cls(**json_decamelized)

    # def _parse_schedule_config(
    #     self, schedule_config: Optional[Union[JobScheduler, dict]]
    # ):
    #     if schedule_config is None:
    #         return None
    #     return (
    #         schedule_config
    #         if isinstance(schedule_config, JobScheduler)
    #         else JobScheduler.from_response_json(schedule_config)
    #     )

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
        if isinstance(self._scheduler_config, JobScheduler):
            scheduler_config = self._scheduler_config.to_dict()
        else:
            scheduler_config = None

        the_dict = {
            "id": self._id,
            "featureStoreId": self._feature_store_id,
            "featureName": self._feature_name,
            "enabled": self._enabled,
            "name": self._name,
            "description": self._description,
            "jobName": self._job_name,
            "featureMonitoringType": self._feature_monitoring_type,
            "schedulerConfig": scheduler_config,
            "detectionWindowConfig": detection_window_config,
            "statisticsComparisonConfig": statistics_comparison_config,
        }

        if self._feature_monitoring_type == "SCHEDULED_STATISTICS":
            return the_dict

        the_dict["referenceWindowConfig"] = reference_window_config
        the_dict["statisticsComparisonConfig"] = statistics_comparison_config
        the_dict["alertConfig"] = self._alert_config

        if self._feature_group_id is not None:
            the_dict["featureGroupId"] = self._feature_group_id
        elif self._feature_view_id is not None:
            the_dict["featureViewId"] = self._feature_view_id

        return the_dict

    def json(self) -> str:
        return json.dumps(self, cls=FeatureStoreEncoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return json.dumps(humps.decamelize(self.to_dict()), indent=2)

    def with_detection_window(
        self,
        time_offset: Optional[str] = None,
        window_length: Optional[str] = None,
        row_percentage: Optional[int] = None,
    ) -> "FeatureMonitoringConfig":
        """Sets the detection window for the feature monitoring job.

        !!! example
            ```python
            # fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)

            # Compute statistics on a regular basis
            fg._enable_scheduled_statistics_fluent(
                name="regular_stats",
                job_frequency="DAILY",
            ).with_detection_window(
                time_offset="1d",
                window_length="1d",
                row_percentage=10,
            ).save()

            # Compute and compare statistics
            fg._enable_feature_monitoring_fluent(
                name="regular_stats",
                feature_name="my_feature",
                job_frequency="DAILY",
            ).with_detection_window(
                time_offset="1d",
                window_length="1d",
                row_percentage=10,
            ).with_reference_window(...).compare_on(...).save()
            ```

        # Arguments
            time_offset: The time offset from the current time to the start of the time window.
            window_length: The length of the time window.
            row_percentage: The percentage of rows to use when computing the statistics.

        # Returns
            `FeatureMonitoringConfig`. The updated FeatureMonitoringConfig object.
        """
        # Setter is using the engine class to perform input validation.
        self.detection_window_config = {
            "window_config_type": "INSERT",
            "time_offset": time_offset,
            "window_length": window_length,
            "row_percentage": row_percentage,
        }

        return self

    def with_reference_window(
        self,
        time_offset: Optional[str] = None,
        window_length: Optional[str] = None,
        row_percentage: Optional[int] = None,
        specific_value: Optional[float] = None,
        training_dataset_id: Optional[int] = None,
    ) -> "FeatureMonitoringConfig":
        """Sets the reference window for the feature monitoring job.

        !!! example
            ```python
            # fethc your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)

            # Setup feature monitoring and a detection window
            my_monitoring_config = fg._enable_feature_monitoring_fluent(...).with_detection_window(...)

            # Simplest reference window is a specific value
            my_monitoring_config.with_reference_window(
                specific_value=0.0,
                row_percentage=10, # optional
            ).compare_on(...).save()

            # Statistics computed on a rolling time window, e.g. same day last week
            my_monitoring_config.with_reference_window(
                time_offset="1w",
                window_length="1d",
            ).compare_on(...).save()

            # Only for feature views: Compare to the statistics computed for one of your training datasets
            # particularly useful if it has been used to train a model currently in production
            my_monitoring_config.with_reference_window(
                training_dataset_id=123,
            ).compare_on(...).save()
            ```

        !!! note
            You must provide a comparison configuration via compare_on(...) before saving the feature monitoring config.

        # Arguments
            specific_value: A specific value to use as reference.
            time_offset: The time offset from the current time to the start of the time window.
            window_length: The length of the time window.
            training_dataset_id: The id of the training dataset to use as reference. For feature view monitoring only.
            row_percentage: The percentage of rows to use when computing the statistics. Defaults to 20%.

        # Returns
            `FeatureMonitoringConfig`. The updated FeatureMonitoringConfig object.
        """
        # Setter is using the engine class to perform input validation.
        self.reference_window_config = {
            "window_config_type": "INSERT",
            "time_offset": time_offset,
            "window_length": window_length,
            "specific_value": specific_value,
            "specific_id": training_dataset_id,
            "row_percentage": row_percentage,
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
            my_monitoring_config = fg._enable_feature_monitoring_fluent(
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
            Detection and reference window must be set prior to comparison configuration.

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

    def alert_on(
        self,
        severity: Optional[str],
        channel: Optional[str],
    ) -> "FeatureMonitoringConfig":
        # TODO: Fake the method for now
        self._alert_config = severity + "_" + channel

        return self

    def save(self):
        """Saves the feature monitoring configuration to the backend.

        !!! example
            ```python
            # fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)

            # Setup feature monitoring and a detection window
            my_monitoring_config = fg._enable_scheduled_statistics(
                name="my_monitoring_config",
            ).save()
            ```

        # Returns
            `FeatureMonitoringConfig`. The saved FeatureMonitoringConfig object.
        """
        self = self._feature_monitoring_config_engine.save(self)
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
        self = self._feature_monitoring_config_engine.update(self)
        return self

    def run_job(self):
        """Trigger the monitoring job which computes statistics on detection and reference window.

        !!! example
            ```python3
            # fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)

            # fetch registered config by name
            my_monitoring_config = fg._get_feature_monitoring_configs(name="my_monitoring_config")

            # trigger the job which computes statistics on detection and reference window
            my_monitoring_config.run_job()
            ```

        # Raises
            `FeatureStoreException`: If the feature monitoring config has not been saved.

        # Returns
            `Job`. A handle for the job computing the statistics.
        """
        if not self._id:
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
            my_monitoring_config = fg._get_feature_monitoring_configs(name="my_monitoring_config")

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
        if not self._id:
            raise FeatureStoreException(
                "Feature monitoring config must be registered via `.save()` before fetching"
                "the associated job."
            )

        return self._feature_monitoring_config_engine.get_monitoring_job(
            job_name=self.job_name
        )

    # TODO: Add read-only setters to match update() method
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
    def feature_view_id(self) -> Optional[int]:
        return self._feature_view_id

    @property
    def feature_name(self) -> str:
        return self._feature_name

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name: str):
        if self._id:
            raise AttributeError("The name of a registered config is read-only.")
        elif not isinstance(name, str):
            raise TypeError("name must be of type str")
        self._name = name

    @property
    def description(self) -> Optional[str]:
        return self._description

    @property
    def job_name(self) -> Optional[str]:
        return self._job_name

    @property
    def enabled(self) -> bool:
        return self._enabled

    @enabled.setter
    def enabled(self, enabled: bool):
        if not isinstance(enabled, bool):
            raise TypeError("enabled must be of type bool")
        if self._id is not None:
            self._feature_monitoring_config_engine.pause_or_resume_monitoring(
                config_id=self._id, enabled=enabled
            )
        self._enabled = enabled

    @property
    def feature_monitoring_type(self) -> Optional[str]:
        return self._feature_monitoring_type

    @property
    def alert_config(self) -> Optional[str]:
        return self._alert_config

    @property
    def scheduler_config(self) -> Optional[JobScheduler]:
        return self._scheduler_config

    @scheduler_config.setter
    def scheduler_config(self, scheduler_config):
        if isinstance(scheduler_config, JobScheduler):
            self._scheduler_config = scheduler_config
        elif isinstance(scheduler_config, dict):
            self._scheduler_config = (
                self._feature_monitoring_config_engine.build_job_scheduler(
                    **scheduler_config
                )
            )
        elif scheduler_config is None:
            self._scheduler_config = scheduler_config
        else:
            raise TypeError(
                "scheduler_config must be of type JobScheduler, dict or None"
            )

    @property
    def detection_window_config(self) -> MonitoringWindowConfig:
        return self._detection_window_config

    @detection_window_config.setter
    def detection_window_config(
        self,
        detection_window_config: Optional[
            Union[MonitoringWindowConfig, Dict[str, Any]]
        ],
    ):
        if isinstance(detection_window_config, MonitoringWindowConfig):
            self._detection_window_config = detection_window_config
        elif isinstance(detection_window_config, dict):
            self._detection_window_config = (
                self._feature_monitoring_config_engine.build_monitoring_window_config(
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
    def reference_window_config(self) -> MonitoringWindowConfig:
        return self._reference_window_config

    @reference_window_config.setter
    def reference_window_config(
        self,
        reference_window_config: Optional[
            Union[MonitoringWindowConfig, Dict[str, Any]]
        ] = None,
    ):
        """Sets the reference window for monitoring."""
        # TODO: improve setter documentation
        if (
            self._feature_monitoring_type == "SCHEDULED_STATISTICS"
            and reference_window_config is not None
        ):
            raise AttributeError(
                "reference_window_config is only available for feature monitoring"
                " not for scheduled statistics."
            )
        if isinstance(reference_window_config, MonitoringWindowConfig):
            self._reference_window_config = reference_window_config
        elif isinstance(reference_window_config, dict):
            self._reference_window_config = (
                self._feature_monitoring_config_engine.build_monitoring_window_config(
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
            self._feature_monitoring_type == "SCHEDULED_STATISTICS"
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

    # TODO: Should the comparison config parameters be exposed as properties?
    # @property
    # def threshold(self) -> Optional[float]:
    #     if self._feature_monitoring_type == "SCHEDULED_STATISTICS":
    #         raise AttributeError(
    #             "threshold is only available for feature monitoring"
    #             " not for scheduled statistics."
    #         )
    #     if isinstance(self._statistics_comparison_config, dict):
    #         return self._statistics_comparison_config.get("threshold", None)
    #     else:
    #         return None

    # @threshold.setter
    # def threshold(self, threshold: Optional[float]):
    #     if self._feature_monitoring_type == "SCHEDULED_STATISTICS":
    #         raise AttributeError(
    #             "threshold setter can only be used when monitoring "
    #             "uses a reference window"
    #         )
    #     if isinstance(self._statistics_comparison_config, dict):
    #         if threshold is None or isinstance(threshold, float):
    #             self._statistics_comparison_config["threshold"] = threshold
    #         else:
    #             raise TypeError("threshold must be of type float or None")
    #     else:
    #         raise AttributeError(
    #             "threshold setter can only be used to update an existing"
    #             "value. Use compare_on to set the statistics comparison configuration first."
    #         )

    # @property
    # def metric(self) -> Optional[str]:
    #     if self._feature_monitoring_type == "SCHEDULED_STATISTICS":
    #         raise AttributeError(
    #             "metric is only available for feature monitoring"
    #             " not for scheduled statistics."
    #         )
    #     if isinstance(self._statistics_comparison_config, dict):
    #         return self._statistics_comparison_config.get("metric", None)
    #     else:
    #         return None

    # @metric.setter
    # def metric(self, metric: Optional[str]):
    #     raise AttributeError("metric field cannot be updated.")

    # @property
    # def relative(self):
    #     if self._feature_monitoring_type == "SCHEDULED_STATISTICS":
    #         raise AttributeError(
    #             "relative is only available for feature monitoring"
    #             " not for scheduled statistics."
    #         )
    #     if isinstance(self._statistics_comparison_config, dict):
    #         return self._statistics_comparison_config.get("relative", None)
    #     else:
    #         return None

    # @relative.setter
    # def relative(self, relative: Optional[bool]):
    #     raise AttributeError(
    #         "relative field cannot be updated. Use compare_on "
    #         "to set the statistics comparison configuration."
    #     )

    # @property
    # def strict(self) -> Optional[bool]:
    #     if self._feature_monitoring_type == "SCHEDULED_STATISTICS":
    #         raise AttributeError(
    #             "strict is only available for feature monitoring"
    #             " not for scheduled statistics."
    #         )
    #     if isinstance(self._statistics_comparison_config, dict):
    #         return self._statistics_comparison_config.get("strict", False)
    #     else:
    #         return None

    # @strict.setter
    # def strict(self, strict: Optional[bool]):
    #     if self._feature_monitoring_type == "SCHEDULED_STATISTICS":
    #         raise AttributeError(
    #             "strict setter can only be used when monitoring "
    #             "uses a reference window"
    #         )
    #     if isinstance(self._statistics_comparison_config, dict):
    #         if isinstance(strict, bool):
    #             self._statistics_comparison_config["strict"] = strict
    #         else:
    #             raise TypeError("strict must be of type bool")
    #     else:
    #         raise AttributeError(
    #             "threshold setter can only be used to update an existing"
    #             "value. Use compare_on to set the statistics comparison configuration first."
    #         )
