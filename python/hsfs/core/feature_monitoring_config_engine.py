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

from typing import Any, Dict, Hashable, Optional, Tuple
import pandas as pd
from datetime import datetime, timedelta

from hsfs.core.feature_monitoring_config import FeatureMonitoringConfig
from hsfs.core.feature_monitoring_config_api import FeatureMonitoringConfigApi
from hsfs.core.feature_monitoring_result_engine import FeatureMonitoringResultEngine
from hsfs.core.feature_monitoring_result import FeatureMonitoringResult

from hsfs.core.job import Job

DEFAULT_REFERENCE_STATS_ID = 234


class FeatureMonitoringConfigEngine:
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

        self._feature_monitoring_config_api = FeatureMonitoringConfigApi(
            feature_store_id=feature_store_id,
            feature_group_id=feature_group_id,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
        )

    def enable_descriptive_statistics_monitoring(
        self,
        name: str,
        feature_name: str,
        detection_window_config: Dict[str, Any],
        scheduler_config: Optional[str] = None,
        description: Optional[str] = None,
    ) -> FeatureMonitoringConfig:
        """Enable descriptive statistics monitoring for a feature.

        Args:
            name: str, required
                Name of the monitoring configuration.
            feature_name: str, required
                Name of the feature to monitor.
            detection_window_config: Dict[str, Any], required
                Configuration of the detection window.
            scheduler_config: str, optional
                Configuration of the scheduler.
            description: str, optional
                Description of the monitoring configuration.

        Returns:
            FeatureMonitoringConfig
        """
        config = self.build_stats_monitoring_only_config(
            name=name,
            feature_name=feature_name,
            detection_window_config=detection_window_config,
            scheduler_config=scheduler_config,
            description=description,
        )

        monitoring_job = self.setup_monitoring_job(
            config_name=config.name,
        )
        config._job_id = monitoring_job.id
        config._job_name = monitoring_job.name

        return self._feature_monitoring_config_api.create(
            fm_config=config,
        )

    def enable_feature_monitoring_config(
        self,
        feature_name: str,
        name: str,
        detection_window_config: Dict[str, Any],
        reference_window_config: Dict[str, Any],
        statistics_comparison_config: Dict[str, Any],
        alert_config: str,
        scheduler_config: str,
        description: Optional[str] = None,
    ) -> FeatureMonitoringConfig:
        """Enable feature monitoring for a feature.

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
            alert_config: str, required
                Configuration of the alert.
            scheduler_config: str, required
                Configuration of the scheduler.
            description: str, optional
                Description of the monitoring configuration.

        Returns:
            FeatureMonitoringConfig The registered monitoring configuration.
        """

        config = self.build_feature_monitoring_config(
            name=name,
            feature_name=feature_name,
            detection_window_config=detection_window_config,
            reference_window_config=reference_window_config,
            statistics_comparison_config=statistics_comparison_config,
            scheduler_config=scheduler_config,
            alert_config=alert_config,
            description=description,
        )

        monitoring_job = self.setup_monitoring_job(
            config_name=config.name,
        )
        config._job_id = monitoring_job.id
        config._job_name = monitoring_job.name

        return self._feature_monitoring_config_api.create(
            fm_config=config,
        )

    def build_monitoring_window_config(
        self,
        window_config_type: str,
        time_offset: Optional[str] = None,
        window_length: Optional[str] = None,
        specific_value: Optional[float] = None,
        specific_id: Optional[int] = None,
        row_percentage: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Builds a monitoring window config dictionary.

        Args:
            window_config_type: str, required
                Type of the window config, can be either
                `SPECIFIC_VALUE`,`FEATURE_GROUP`,`INSERT`,`BATCH` or `FEATURE_VIEW`.
            time_offset: str, optional
                monitoring window start time is computed as "now - time_offset".
            window_length: str, optional
                monitoring window end time is computed as
                    "now - time_offset + window_length".
            specific_value: float, optional
                Specific value instead of a statistics computed on data.
            specific_id: int, optional
                Specific id of an entity that has fixed

        """

        return {
            "window_config_type": window_config_type,
            "time_offset": time_offset,
            "window_length": window_length,
            "specific_id": specific_id,
            "specific_value": specific_value,
            "row_percentage": row_percentage,
        }

    def build_stats_monitoring_only_config(
        self,
        name: str,
        feature_name: str,
        detection_window_config: Dict[str, Any],
        scheduler_config: Dict[str, Any],
        description: Optional[str] = None,
    ) -> FeatureMonitoringConfig:
        """Builds a feature monitoring config for descriptive statistics only.

        Args:
            name: str, required
                Name of the monitoring configuration.
            feature_name: str, required
                Name of the feature to monitor.
            detection_window_config: Dict[str, Any], required
                Configuration of the detection window.
            scheduler_config: str, required
                Configuration of the scheduler.
            description: str, optional
                Description of the monitoring configuration.

        Returns:
            FeatureMonitoringConfig The monitoring configuration.
        """

        return FeatureMonitoringConfig(
            feature_store_id=self._feature_store_id,
            feature_group_id=self._feature_group_id,
            feature_view_id=self._feature_view_id,
            feature_name=feature_name,
            name=name,
            description=description,
            feature_monitoring_type="DESCRIPTIVE_STATISTICS",
            detection_window_config=detection_window_config,
            scheduler_config=scheduler_config,
            enabled=True,
            alert_config=None,
            reference_window_config=None,
            statistics_comparison_config=None,
        )

    def build_feature_monitoring_config(
        self,
        feature_name: str,
        name: str,
        detection_window_config: Dict[str, Any],
        reference_window_config: Dict[str, Any],
        statistics_comparison_config: Dict[str, Any],
        scheduler_config: str,
        alert_config: str,
        description: Optional[str] = None,
    ) -> FeatureMonitoringConfig:
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
            scheduler_config: str, required
                Configuration of the scheduler.
            alert_config: str, required
                Configuration of the alert.
            description: str, optional
                Description of the monitoring configuration.
        """

        return FeatureMonitoringConfig(
            feature_store_id=self._feature_store_id,
            feature_group_id=self._feature_group_id,
            feature_view_id=self._feature_view_id,
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

    def setup_monitoring_job(
        self,
        config_name: str,
    ) -> Job:
        """Setup a feature monitoring job based on the entity and config name.

        Args:
            config_name: name of the monitoring config.

        Returns:
            Job: monitoring job object.
        """

        return self._feature_monitoring_config_api.setup_feature_monitoring_job(
            config_name=config_name,
        )

    def trigger_monitoring_job(
        self,
        config_id: int,
    ) -> Job:
        """Trigger a feature monitoring job based on the entity and config id.

        Args:
            config_id: id of the monitoring config.

        Returns:
            Job object.
        """
        return self._feature_monitoring_config_api.trigger_feature_monitoring_job(
            config_id=config_id,
        )

    def run_feature_monitoring(
        self, entity, config_name: str, result_engine: FeatureMonitoringResultEngine
    ) -> FeatureMonitoringResult:
        """Run by feature monitoring job, monitoring is based on the config id.

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

        detection_stats, detection_stats_id = self.run_single_window_monitoring(
            entity=entity,
            monitoring_window_config=config.detection_window_config,
            feature_name=config.feature_name,
            check_existing=False,
        )

        if config.reference_window_config is not None:
            reference_stats, reference_stats_id = self.run_single_window_monitoring(
                entity=entity,
                monitoring_window_config=config.reference_window_config,
                feature_name=config.feature_name,
                check_existing=True,
            )

        result = result_engine.run_and_save_statistics_comparison(
            detection_stats_id=detection_stats_id,
            reference_stats_id=reference_stats_id if reference_stats_id else None,
            detection_stats=detection_stats,
            reference_stats=reference_stats if reference_stats else None,
            fm_config=config,
        )

        return result

    def run_single_window_monitoring(
        self,
        entity,
        monitoring_window_config: Dict[str, Any],
        feature_name: str,
        check_existing: bool = False,
    ) -> Tuple[Dict[str, Any], int]:
        """Run monitoring for a single window.

        Args:
            entity: FeatureStore: Feature store to fetch the entity to monitor.
            monitoring_window_config: Dict[str, Any]: Monitoring window config.
            feature_name: str: Name of the feature to monitor.
            check_existing: bool: Whether to check for existing stats.

        Returns:
            Tuple[Dict[str, Any], int]: Tuple of stats and stats id.
        """
        if check_existing:
            registered_stats_id = self.check_for_registered_stats(
                monitoring_window_config
            )
            if registered_stats_id is not None:
                return {}, registered_stats_id
        elif monitoring_window_config["window_config_type"] == "SPECIFIC_VALUE":
            return {
                "specific_value": monitoring_window_config["specifc_value"]
            }, DEFAULT_REFERENCE_STATS_ID

        # Fetch the actual data for which to compute statistics based on row_percentage and time window
        entity_df = self.fetch_entity_data_based_on_monitoring_window_config(
            entity=entity,
            feature_name=feature_name,
            monitoring_window_config=monitoring_window_config,
        )

        # This method should be implemented on a statistics_compute_engine rather than here
        stats, registered_stats_id = self.compute_and_upload_statistics(entity_df)

        return stats, registered_stats_id

    def compute_and_upload_statistics(
        self, entity_df: pd.DataFrame
    ) -> Tuple[Dict[Hashable, Any], int]:
        # Dummy method for now
        # This method should be implemented on a statistics_compute_engine rather than here
        # Or potentially just in the python/spark engines
        stats = entity_df.describe()
        print("Computing and faking statistics upload... ")
        registered_stats_id = 22

        return stats.to_dict(), registered_stats_id

    def check_for_registered_stats(
        self, monitoring_window_config: Dict[str, Any]
    ) -> Optional[int]:
        # Dummy method for now
        print(
            "Checking for registered stats for window config: ",
            monitoring_window_config.get("id", "unregistered"),
        )
        return None

    def fetch_entity_data_based_on_monitoring_window_config(
        self,
        entity,  #: Union[feature_group.FeatureGroup, feature_view.FeatureView],
        feature_name: str,
        monitoring_window_config: Dict[str, Any],
    ) -> pd.DataFrame:
        """Fetch the entity data based on the monitoring window config.

        Args:
            entity: Union[FeatureGroup, FeatureView]: Entity to monitor.
            feature_name: str: Name of the feature to monitor.
            monitoring_window_config: Dict[str, Any]: Monitoring window config.

        Returns:
            pd.DataFrame: Entity data.
        """
        # Lots of work to be done here to suport all the different cases
        time_offset = self.time_range_str_to_time_delta(
            monitoring_window_config["time_offset"]
        )
        window_length = self.time_range_str_to_time_delta(
            monitoring_window_config["window_length"]
        )

        return (
            entity.select(features=[feature_name])
            .as_of(
                exclude_until=datetime.now() - time_offset,
                wallclock_time=datetime.now() - time_offset + window_length,
            )
            .read(dataframe_type="pandas")
        )

    def time_range_str_to_time_delta(self, time_range: str) -> timedelta:
        # Dummy method for now
        if time_range == "1m":
            time_offset = timedelta(months=1)
        elif time_range == "1w":
            time_offset = timedelta(weeks=1)
        elif time_range == "1d":
            time_offset = timedelta(days=1)
        elif time_range == "1h":
            time_offset = timedelta(hours=1)

        return time_offset
